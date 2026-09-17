// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Scrape-time aggregation of per-object metrics into bounded series.
//!
//! A subsystem that tracks a quantity per object (per shard, per replica) and
//! exports one series per object grows its scrape body with the object count.
//! [`DistributionCollector`] reports the same quantity at a cardinality set by
//! the number of metrics instead: a histogram of the quantity across objects,
//! plus the `K` objects currently holding the largest values.
//!
//! Nothing is maintained incrementally. Each `collect` re-derives both from the
//! caller's live values, so the distribution and the top-K are exact even for a
//! quantity that moves in both directions, and the objects' update path stays a
//! plain atomic store with no heap and no lock per update.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

use prometheus::core::{AtomicU64, Collector, Desc, GenericGaugeVec};
use prometheus::proto::MetricFamily;
use prometheus::{Histogram, HistogramOpts, Opts};

use crate::cast::CastLossy;

/// One aggregated family: a distribution over objects, optionally with top-K.
#[derive(Debug, Clone)]
pub struct AggregatedFamily {
    /// Base metric name. The emitted families are `<name>_distribution` and,
    /// when `top_k` is set, `<name>_topk`.
    pub name: String,
    /// Help text, shared by both emitted families.
    pub help: String,
    /// Upper bounds of the distribution histogram's buckets, ascending.
    pub buckets: Vec<f64>,
    /// When set, also emit the `K` objects holding the largest current values,
    /// identified by the collector's key labels.
    pub top_k: Option<usize>,
}

/// A walk over the caller's live objects, reporting each object's key label
/// values and its per-family values.
///
/// Both slices are positional: key values align with the collector's
/// `key_labels`, metric values with its `families`. The walk is invoked once per
/// scrape and must report every live object exactly once.
///
/// The consumer only appends to local `Vec`s, so a walk that locks the object
/// collection holds that lock for the snapshot alone. Bucketing and top-K
/// selection run after the walk returns.
pub type ObjectWalk = Arc<dyn Fn(&mut dyn FnMut(&[&str], &[u64])) + Send + Sync>;

/// Emits a bounded distribution, and optionally a top-K, per [`AggregatedFamily`]
/// by folding a caller-supplied walk over live objects at scrape time.
pub struct DistributionCollector {
    families: Vec<AggregatedFamily>,
    key_labels: Vec<String>,
    walk: ObjectWalk,
    /// Descriptors of the `_distribution` histograms.
    ///
    /// The `_topk` families are deliberately absent. Prometheus uses `desc` only
    /// to detect collisions when a collector is registered, not to validate what
    /// `collect` returns, so leaving them undeclared lets a key that falls out of
    /// the top-K simply stop being emitted rather than linger at a stale value.
    descs: Vec<Desc>,
}

impl DistributionCollector {
    /// Creates a collector over `families`, identifying top-K objects by
    /// `key_labels`.
    ///
    /// Panics if a family's name, help or buckets are not valid Prometheus
    /// options, so a misconfigured family fails at construction rather than
    /// silently dropping series at scrape time.
    pub fn new(families: Vec<AggregatedFamily>, key_labels: Vec<String>, walk: ObjectWalk) -> Self {
        let descs = families
            .iter()
            .map(|family| distribution(family).desc()[0].clone())
            .collect();
        DistributionCollector {
            families,
            key_labels,
            walk,
            descs,
        }
    }
}

impl Debug for DistributionCollector {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributionCollector")
            .field("families", &self.families)
            .field("key_labels", &self.key_labels)
            .finish_non_exhaustive()
    }
}

impl Collector for DistributionCollector {
    fn desc(&self) -> Vec<&Desc> {
        self.descs.iter().collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let stride = self.families.len();
        let key_arity = self.key_labels.len();

        // Snapshot first: `keys` and `values` are flat, object-major, so family
        // `i` of object `o` is `values[o * stride + i]`.
        let mut keys: Vec<String> = Vec::new();
        let mut values: Vec<u64> = Vec::new();
        (self.walk)(&mut |object_keys, object_values| {
            assert_eq!(
                object_keys.len(),
                key_arity,
                "walk reported wrong key arity"
            );
            assert_eq!(
                object_values.len(),
                stride,
                "walk reported wrong value arity"
            );
            keys.extend(object_keys.iter().map(|key| key.to_string()));
            values.extend_from_slice(object_values);
        });
        let objects = if stride == 0 {
            0
        } else {
            values.len() / stride
        };

        let mut out = Vec::with_capacity(self.families.len() * 2);
        for (i, family) in self.families.iter().enumerate() {
            let column = (0..objects).map(|o| values[o * stride + i]);

            let histogram = distribution(family);
            for value in column.clone() {
                histogram.observe(f64::cast_lossy(value));
            }
            out.extend(histogram.collect());

            let Some(k) = family.top_k else { continue };
            let gauges = top_k_gauges(family, &self.key_labels);
            for (object, value) in top_k(column, k) {
                let labels: Vec<&str> = keys[object * key_arity..(object + 1) * key_arity]
                    .iter()
                    .map(String::as_str)
                    .collect();
                gauges.with_label_values(&labels).set(value);
            }
            out.extend(gauges.collect());
        }
        out
    }
}

fn distribution(family: &AggregatedFamily) -> Histogram {
    let opts = HistogramOpts::new(format!("{}_distribution", family.name), family.help.clone())
        .buckets(family.buckets.clone());
    Histogram::with_opts(opts).expect("aggregated family has valid histogram options")
}

fn top_k_gauges(family: &AggregatedFamily, key_labels: &[String]) -> GenericGaugeVec<AtomicU64> {
    let opts = Opts::new(format!("{}_topk", family.name), family.help.clone());
    let key_labels: Vec<&str> = key_labels.iter().map(String::as_str).collect();
    GenericGaugeVec::new(opts, &key_labels).expect("aggregated family has valid gauge options")
}

/// The `k` largest `(object index, value)` pairs, in no particular order. Ties
/// break towards the lower index, so more than `k` equal values still select the
/// same objects from one scrape to the next instead of churning series.
fn top_k(values: impl Iterator<Item = u64>, k: usize) -> Vec<(usize, u64)> {
    if k == 0 {
        return Vec::new();
    }
    // A max-heap of `Reverse((value, Reverse(index)))` pops the smallest value,
    // and among equal values the largest index, which is exactly what must be
    // evicted to keep the `k` largest under the tie rule above.
    let mut heap = BinaryHeap::with_capacity(k + 1);
    for (index, value) in values.enumerate() {
        heap.push(Reverse((value, Reverse(index))));
        if heap.len() > k {
            heap.pop();
        }
    }
    heap.into_iter()
        .map(|Reverse((value, Reverse(index)))| (index, value))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use prometheus::proto::MetricFamily;

    use super::*;
    use crate::metrics::MetricsRegistry;

    /// Objects named `key<i>` carrying a single value each, mutable between
    /// scrapes so a test can churn the population.
    #[derive(Default)]
    struct Objects(Mutex<Vec<(String, u64)>>);

    impl Objects {
        fn walk(self: &Arc<Self>) -> ObjectWalk {
            let objects = Arc::clone(self);
            Arc::new(move |report: &mut dyn FnMut(&[&str], &[u64])| {
                for (key, value) in objects.0.lock().expect("mutex poisoned").iter() {
                    report(&[key.as_str()], &[*value]);
                }
            })
        }

        fn set(&self, values: impl IntoIterator<Item = u64>) {
            *self.0.lock().expect("mutex poisoned") = values
                .into_iter()
                .enumerate()
                .map(|(i, value)| (format!("key{i}"), value))
                .collect();
        }
    }

    fn family(top_k: Option<usize>) -> AggregatedFamily {
        AggregatedFamily {
            name: "mz_test_object_size".to_string(),
            help: "size of each test object".to_string(),
            buckets: vec![1.0, 10.0, 100.0],
            top_k,
        }
    }

    fn registered(top_k: Option<usize>) -> (MetricsRegistry, Arc<Objects>) {
        let objects = Arc::new(Objects::default());
        let registry = MetricsRegistry::new();
        registry.register_collector(DistributionCollector::new(
            vec![family(top_k)],
            vec!["key".to_string()],
            objects.walk(),
        ));
        (registry, objects)
    }

    fn find<'a>(families: &'a [MetricFamily], name: &str) -> Option<&'a MetricFamily> {
        families.iter().find(|f| f.name() == name)
    }

    #[crate::test]
    fn distribution_buckets_are_exact() {
        let (registry, objects) = registered(None);
        objects.set([0, 1, 5, 10, 50, 100, 1000]);

        let gathered = registry.gather();
        let distribution = find(&gathered, "mz_test_object_size_distribution")
            .expect("distribution family is emitted");
        let histogram = distribution.get_metric()[0].get_histogram();

        assert_eq!(histogram.get_sample_count(), 7);
        assert_eq!(histogram.get_sample_sum(), 1166.0);
        let buckets: Vec<_> = histogram
            .get_bucket()
            .iter()
            .map(|b| (b.upper_bound(), b.cumulative_count()))
            .collect();
        assert_eq!(buckets, vec![(1.0, 2), (10.0, 4), (100.0, 6)]);
    }

    #[crate::test]
    fn topk_selects_largest_in_order() {
        let (registry, objects) = registered(Some(10));
        objects.set(1..=100);

        let gathered = registry.gather();
        let top = find(&gathered, "mz_test_object_size_topk").expect("top-K family is emitted");

        // Series carry no order, so compare the selected set sorted by value.
        let mut series: Vec<_> = top
            .get_metric()
            .iter()
            .map(|m| {
                let label = &m.get_label()[0];
                assert_eq!(label.name(), "key");
                (m.get_gauge().value(), label.value().to_string())
            })
            .collect();
        series.sort_by(|a, b| a.0.total_cmp(&b.0));
        let expected: Vec<_> = (91..=100)
            .map(|value: u64| (f64::cast_lossy(value), format!("key{}", value - 1)))
            .collect();
        assert_eq!(series, expected);
    }

    #[crate::test]
    fn topk_membership_churns_without_stale_series() {
        let (registry, objects) = registered(Some(1));
        objects.set([10, 1]);
        let gathered = registry.gather();
        let top = find(&gathered, "mz_test_object_size_topk").expect("top-K family is emitted");
        assert_eq!(top.get_metric().len(), 1);
        assert_eq!(top.get_metric()[0].get_label()[0].value(), "key0");

        // key1 overtakes key0; key0's series must be gone, not zeroed.
        objects.set([10, 20]);
        let gathered = registry.gather();
        let top = find(&gathered, "mz_test_object_size_topk").expect("top-K family is emitted");
        assert_eq!(top.get_metric().len(), 1);
        assert_eq!(top.get_metric()[0].get_label()[0].value(), "key1");
    }

    #[crate::test]
    fn cardinality_is_flat_in_object_count() {
        let series_count = |registry: &MetricsRegistry| {
            registry
                .gather()
                .iter()
                .map(|f| f.get_metric().len())
                .sum::<usize>()
        };

        let (registry, objects) = registered(Some(10));
        objects.set(0..10);
        let small = series_count(&registry);
        objects.set(0..10_000);
        assert_eq!(series_count(&registry), small);
    }
}
