use super::{Accounting, EventCounts, Latencies, Report, Throughput, Timing, distribution};
use crate::{
    LoadError, config,
    model::{Counters, RunData, Sample},
};
use std::collections::HashSet;

fn invalid(message: impl Into<String>) -> LoadError {
    LoadError::InvalidData(message.into())
}
fn add(target: &mut u64, value: u64) -> Result<(), LoadError> {
    *target = target
        .checked_add(value)
        .ok_or(LoadError::Capacity("metric counter overflow"))?;
    Ok(())
}
fn merge(total: &mut Counters, c: &Counters) -> Result<(), LoadError> {
    add(&mut total.scheduled, c.scheduled)?;
    add(&mut total.attempted, c.attempted)?;
    add(&mut total.write_completed, c.write_completed)?;
    add(&mut total.puback_received, c.puback_received)?;
    add(&mut total.pubrec_received, c.pubrec_received)?;
    add(&mut total.pubcomp_received, c.pubcomp_received)?;
    add(&mut total.delivered_unique, c.delivered_unique)?;
    add(&mut total.duplicates, c.duplicates)?;
    add(&mut total.unexpected, c.unexpected)?;
    add(&mut total.invalid_payload, c.invalid_payload)?;
    add(&mut total.completed, c.completed)
}
fn collect(
    data: &RunData,
    select: impl Fn(&Sample) -> Option<u64>,
) -> Result<super::Distribution, LoadError> {
    let size = data.pairs.iter().try_fold(0_usize, |n, pair| {
        n.checked_add(pair.samples.len())
            .ok_or(LoadError::Capacity("sample count overflow"))
    })?;
    let mut values = Vec::new();
    values
        .try_reserve_exact(size)
        .map_err(|_| LoadError::Capacity("latency sort allocation"))?;
    for sample in data.pairs.iter().flat_map(|p| &p.samples) {
        if let Some(value) = select(sample) {
            values.push(value);
        }
    }
    distribution(values)
}
fn tally(
    events: &mut EventCounts,
    sample: &Sample,
    include: impl Fn(u64) -> bool,
) -> Result<(), LoadError> {
    if include(sample.send_started_ns) {
        add(&mut events.attempted, 1)?;
    }
    if sample.write_completed_ns.is_some_and(&include) {
        add(&mut events.write_completed, 1)?;
    }
    if sample.delivery_ns.is_some_and(&include) {
        add(&mut events.delivered_unique, 1)?;
    }
    if sample.ack_ns.is_some_and(&include)
        && let Some(n) = &mut events.source_completed
    {
        add(n, 1)?;
    }
    if sample.completed_ns.is_some_and(include) {
        add(&mut events.fully_completed, 1)?;
    }
    Ok(())
}
fn validate_sample(sample: &Sample, data: &RunData) -> Result<(), LoadError> {
    if sample.scheduled_ns < data.measured_start_ns
        || sample.scheduled_ns >= data.measured_end_ns
        || sample.send_started_ns < sample.scheduled_ns
        || sample.send_started_ns >= data.measured_end_ns
    {
        return Err(invalid(
            "sample scheduled/send timestamp outside offering interval or sent before schedule",
        ));
    }
    for timestamp in [
        sample.write_completed_ns,
        sample.ack_ns,
        sample.delivery_ns,
        sample.completed_ns,
    ]
    .into_iter()
    .flatten()
    {
        if timestamp < sample.send_started_ns || timestamp > data.finished_ns {
            return Err(invalid("sample event precedes send or exceeds run finish"));
        }
    }
    if sample.send_started_ns > data.finished_ns {
        return Err(invalid("sample send exceeds run finish"));
    }
    // The paired actor reads delivery/source events only after send() has fully
    // completed. Validate each record, so another sample's write cannot mask an
    // impossible acknowledgement or delivery in aggregate counters.
    if sample.write_completed_ns.is_none()
        && (sample.ack_ns.is_some() || sample.delivery_ns.is_some())
    {
        return Err(invalid("acknowledgement or delivery without full write"));
    }
    if data.config.qos == 0 && sample.ack_ns.is_some() {
        return Err(invalid("QoS0 cannot have a source acknowledgement"));
    }
    if let Some(completed) = sample.completed_ns {
        let write = sample
            .write_completed_ns
            .ok_or_else(|| invalid("completion without full write"))?;
        let delivered = sample
            .delivery_ns
            .ok_or_else(|| invalid("completion without unique delivery"))?;
        let source = if data.config.qos == 0 {
            write
        } else {
            sample
                .ack_ns
                .ok_or_else(|| invalid("QoS completion without final acknowledgement"))?
        };
        if completed < write.max(delivered).max(source) {
            return Err(invalid("completion precedes required events"));
        }
    }
    Ok(())
}

/// Compute exact observations for the measured publication cohort, preserving censored work.
/// Expected failed/incomplete runs return an unsuccessful report; malformed instrumentation
/// returns a typed error instead of producing plausible but incorrect metrics.
pub fn summarize(data: &RunData) -> Result<Report, LoadError> {
    config::validate(&data.config)?;
    let offering_ns = data
        .measured_end_ns
        .checked_sub(data.measured_start_ns)
        .filter(|n| *n > 0)
        .ok_or_else(|| invalid("measurement interval must be positive"))?;
    let mut issues = data.failures.clone();
    let mut warnings = Vec::new();
    if data.finished_ns < data.measured_end_ns {
        issues.push("run finished before the offering interval ended".into());
    }
    if offering_ns != data.config.duration_ms * 1_000_000 {
        issues.push("offering interval differs from configured duration".into());
    }
    if data.pairs.len() != data.config.pairs {
        issues.push("not all configured pairs returned data".into());
    }
    let mut ids = HashSet::new();
    let mut total = Counters::default();
    let mut sample_count = 0_u64;
    let mut failed_pairs = 0_u64;
    let source = (data.config.qos > 0).then_some(0);
    let mut window = EventCounts {
        source_completed: source,
        ..EventCounts::default()
    };
    let mut late = window.clone();
    let mut all = window.clone();
    for pair in &data.pairs {
        if usize::try_from(pair.pair).map_or(true, |p| p >= data.config.pairs)
            || !ids.insert(pair.pair)
        {
            return Err(invalid("duplicate or out-of-range pair identifier"));
        }
        if let Some(error) = &pair.error {
            add(&mut failed_pairs, 1)?;
            issues.push(format!("pair {}: {error}", pair.pair));
        }
        let count = u64::try_from(pair.samples.len())
            .map_err(|_| LoadError::Capacity("sample count overflow"))?;
        if count > data.config.quota(pair.pair) {
            return Err(LoadError::Capacity("pair exceeded retained sample quota"));
        }
        add(&mut sample_count, count)?;
        if sample_count > data.config.max_messages {
            return Err(LoadError::Capacity("global retained sample quota exceeded"));
        }
        let mut derived = EventCounts {
            source_completed: source,
            ..EventCounts::default()
        };
        let mut sequences = HashSet::new();
        for sample in &pair.samples {
            if sample.pair != pair.pair || !sequences.insert(sample.sequence) {
                return Err(invalid("duplicate or wrong-pair sample identity"));
            }
            validate_sample(sample, data)?;
            tally(&mut derived, sample, |_| true)?;
            tally(&mut all, sample, |_| true)?;
            tally(&mut window, sample, |t| {
                t >= data.measured_start_ns && t < data.measured_end_ns
            })?;
            tally(&mut late, sample, |t| t >= data.measured_end_ns)?;
        }
        if let (Some(min), Some(max)) = (sequences.iter().min(), sequences.iter().max()) {
            let span = max.checked_sub(*min).and_then(|n| n.checked_add(1));
            if span != Some(count) {
                return Err(invalid(
                    "measured sequence gap: sample records may have been omitted",
                ));
            }
        }
        let c = &pair.counters;
        if c.attempted != count
            || c.write_completed != derived.write_completed
            || c.delivered_unique != derived.delivered_unique
            || c.completed != derived.fully_completed
        {
            return Err(invalid(
                "record count and pair counters disagree; samples may have been dropped",
            ));
        }
        if c.scheduled < c.attempted
            || c.completed > c.write_completed
            || c.delivered_unique > c.write_completed
        {
            return Err(invalid(
                "impossible scheduled/write/delivery/completion counts",
            ));
        }
        match data.config.qos {
            0 if c.puback_received != 0 || c.pubrec_received != 0 || c.pubcomp_received != 0 => {
                return Err(invalid("QoS0 has protocol acknowledgement counters"));
            }
            1 if c.puback_received != derived.source_completed.unwrap_or(0)
                || c.pubrec_received != 0
                || c.pubcomp_received != 0 =>
            {
                return Err(invalid("QoS1 acknowledgement counts disagree"));
            }
            2 if c.pubcomp_received != derived.source_completed.unwrap_or(0)
                || c.puback_received != 0
                || c.pubrec_received < c.pubcomp_received
                || c.pubrec_received > c.attempted =>
            {
                return Err(invalid(
                    "QoS2 final/intermediate acknowledgement counts disagree",
                ));
            }
            _ => {}
        }
        merge(&mut total, c)?;
    }
    let source_count = all.source_completed;
    let not_sent = total.scheduled - total.attempted;
    let not_delivered = total.write_completed - total.delivered_unique;
    let not_confirmed = source_count.map(|n| total.write_completed.saturating_sub(n));
    if source_count.is_some_and(|n| n > total.write_completed) {
        return Err(invalid("source acknowledgements exceed full writes"));
    }
    let incomplete = total.attempted - total.completed;
    if data.config.rate > 0 {
        let expected =
            (u128::from(data.config.rate) * u128::from(offering_ns)).div_ceil(1_000_000_000);
        if u128::from(total.scheduled) != expected {
            issues.push(format!(
                "scheduled count {} differs from absolute-rate plan {expected}",
                total.scheduled
            ));
        }
    }
    if total.attempted == 0 {
        issues.push("no measured messages attempted".into());
    }
    if not_sent > 0 {
        issues.push(format!("{not_sent} scheduled messages were not sent"));
    }
    if incomplete > 0 {
        issues.push(format!(
            "{incomplete} measured attempts did not fully complete"
        ));
    }
    if total.unexpected > 0 || total.invalid_payload > 0 {
        issues.push("unexpected or invalid payload observations".into());
    }
    if total.duplicates > 0 {
        warnings.push(format!(
            "{} duplicate observations excluded from unique delivery and percentiles",
            total.duplicates
        ));
    }
    if total.attempted == data.config.max_messages {
        warnings.push(
            "all configured sample capacity was used; inspect pair errors for truncation".into(),
        );
    }
    let latencies = Latencies {
        schedule_to_send: collect(data, |s| Some(s.send_started_ns - s.scheduled_ns))?,
        send_to_write_completed: collect(data, |s| {
            s.write_completed_ns.map(|t| t - s.send_started_ns)
        })?,
        send_to_delivery: collect(data, |s| s.delivery_ns.map(|t| t - s.send_started_ns))?,
        schedule_to_delivery: collect(data, |s| s.delivery_ns.map(|t| t - s.scheduled_ns))?,
        send_to_source_completed: if data.config.qos == 0 {
            None
        } else {
            Some(collect(data, |s| s.ack_ns.map(|t| t - s.send_started_ns))?)
        },
        send_to_full_completion: collect(data, |s| s.completed_ns.map(|t| t - s.send_started_ns))?,
    };
    let horizon = data
        .finished_ns
        .checked_sub(data.measured_start_ns)
        .filter(|n| *n > 0);
    Ok(Report {
        schema_version: 1,
        success: issues.is_empty(),
        run_id: data.run_id,
        config: data.config.clone(),
        endpoint_mapping: "publisher endpoint=pair%N; subscriber endpoint=(pair+1)%N; N=effective endpoint count",
        configured_endpoint_connections: data
            .config
            .endpoints()
            .iter()
            .enumerate()
            .map(|(index, address)| {
                let n = data.config.endpoints().len();
                super::report::EndpointConnections {
                    address: *address,
                    publishers: (0..data.config.pairs)
                        .filter(|pair| pair % n == index)
                        .count(),
                    subscribers: (0..data.config.pairs)
                        .filter(|pair| (pair + 1) % n == index)
                        .count(),
                }
            })
            .collect(),
        mode: if data.config.rate == 0 {
            "closed_loop"
        } else {
            "absolute_rate"
        },
        load_semantics: if data.config.rate == 0 {
            "closed-loop window credits wait for source completion and unique delivery; no open-loop or coordinated-omission-corrected latency claim"
        } else {
            "absolute-rate intended deadlines; bounded inflight can delay writes; scheduling lag and not-sent messages retained"
        },
        source_completion_definition: match data.config.qos {
            0 => "not applicable: QoS0 has no wire acknowledgement",
            1 => "positive PUBACK received for the published packet identifier",
            _ => {
                "positive final PUBCOMP received; PUBREC is intermediate and never source completion"
            }
        },
        delivery_definition: "validated unique subscriber application delivery; QoS2 recorded at PUBREL; duplicates excluded",
        throughput_definition: "offering-window event timestamps in [start,end); cohort-through-drain includes late events using finish-start denominator; never sum ingress and egress",
        clock: "one-process std::time::Instant epoch",
        percentile_method: "exact nearest rank ceil(q*N), integer nanoseconds, no interpolation",
        timestamp_unit: "nanoseconds",
        timing: Timing {
            measured_start_ns: data.measured_start_ns,
            measured_end_ns: data.measured_end_ns,
            finished_ns: data.finished_ns,
            offering_window_ns: offering_ns,
            observed_cohort_horizon_ns: horizon,
            drain_elapsed_ns: data.finished_ns.saturating_sub(data.measured_end_ns),
        },
        accounting: Accounting {
            counters: total,
            samples_retained: sample_count,
            not_sent,
            not_delivered,
            not_source_confirmed: not_confirmed,
            incomplete_attempts: incomplete,
            failed_pairs,
            configured_pairs: data.config.pairs,
            returned_pairs: data.pairs.len(),
        },
        offering_window_tps: Throughput::new(&window, Some(offering_ns)),
        measured_cohort_tps_through_drain: Throughput::new(&all, horizon),
        offering_window_events: window,
        after_offering_window_events: late,
        latencies,
        issues,
        warnings,
    })
}
