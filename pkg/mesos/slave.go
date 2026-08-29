package mesos

import (
	"encoding/json"
	"fmt"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

func NewSlaveCollector(httpClient *HttpClient, attr map[string]json.RawMessage, userTaskLabelList []string, slaveAttributeLabelList []string) prometheus.Collector {
	defaultTaskLabels := []string{"type"}
	defaultLabels := append(defaultTaskLabels, normaliseLabelList(slaveAttributeLabelList)...)

	sAtt := prometheus.Labels{}
	for _, label := range defaultLabels {
		sAtt[label] = ""
	}
	for key, value := range attr {
		normalisedLabel := normaliseLabel(key)
		if stringInSlice(normalisedLabel, defaultLabels) {
			if attribute, err := attributeString(value); err == nil {
				sAtt[normalisedLabel] = attribute
			}
		}
	}
	subLabels := defaultLabels[1:]

	metrics := map[prometheus.Collector]func(MetricMap, prometheus.Collector) error{
		// CPU/Disk/Mem Resources in free/used
		gauge("slave", "cpus", "Current CPU Resources in cluster.",
			defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["Slave/cpus_percent"]
			if !ok {

				return fmt.Errorf("key Slave/cpus_percent not found in map")

			}
			total, ok := m["Slave/cpus_total"]
			if !ok {

				return fmt.Errorf("key Slave/cpus_total not found in map")

			}
			used, ok := m["Slave/cpus_used"]
			if !ok {

				return fmt.Errorf("key Slave/cpus_used not found in map")

			}
			getLabelValuesFromMap(sAtt, defaultLabels[1:])
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("percent", sAtt, subLabels)...).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			return nil
		},
		gauge("slave", "cpus_revocable", "Current revocable CPU Resources in cluster.",
			defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["Slave/cpus_revocable_percent"]
			if !ok {

				return fmt.Errorf("key Slave/cpus_revocable_percent not found in map")

			}
			total, ok := m["Slave/cpus_revocable_total"]
			if !ok {

				return fmt.Errorf("key Slave/cpus_revocable_total not found in map")

			}
			used, ok := m["Slave/cpus_revocable_used"]
			if !ok {

				return fmt.Errorf("key Slave/cpus_revocable_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("percent", sAtt, subLabels)...).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			return nil
		},
		gauge("slave", "mem", "Current memory Resources in cluster.",
			defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["Slave/mem_percent"]
			if !ok {

				return fmt.Errorf("key Slave/mem_percent not found in map")

			}
			total, ok := m["Slave/mem_total"]
			if !ok {

				return fmt.Errorf("key Slave/mem_total not found in map")

			}
			used, ok := m["Slave/mem_used"]
			if !ok {

				return fmt.Errorf("key Slave/mem_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("percent", sAtt, subLabels)...).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			return nil
		},
		gauge("slave", "mem_revocable", "Current revocable memory Resources in cluster.",
			defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["Slave/mem_revocable_percent"]
			if !ok {

				return fmt.Errorf("key Slave/mem_revocable_percent not found in map")

			}
			total, ok := m["Slave/mem_revocable_total"]
			if !ok {

				return fmt.Errorf("key Slave/mem_revocable_total not found in map")

			}
			used, ok := m["Slave/mem_revocable_used"]
			if !ok {

				return fmt.Errorf("key Slave/mem_revocable_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("percent", sAtt, subLabels)...).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			return nil
		},
		gauge("slave", "gpus", "Current GPU Resources in cluster.",
			defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["Slave/gpus_percent"]
			if !ok {

				return fmt.Errorf("key Slave/gpus_percent not found in map")

			}
			total, ok := m["Slave/gpus_total"]
			if !ok {

				return fmt.Errorf("key Slave/gpus_total not found in map")

			}
			used, ok := m["Slave/gpus_used"]
			if !ok {

				return fmt.Errorf("key Slave/gpus_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("percent", sAtt, subLabels)...).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			return nil
		},
		gauge("slave", "gpus_revocable", "Current revocable GPUS Resources in cluster.",
			defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["Slave/gpus_revocable_percent"]
			if !ok {

				return fmt.Errorf("key Slave/gpus_revocable_percent not found in map")

			}
			total, ok := m["Slave/gpus_revocable_total"]
			if !ok {

				return fmt.Errorf("key Slave/gpus_revocable_total not found in map")

			}
			used, ok := m["Slave/gpus_revocable_used"]
			if !ok {

				return fmt.Errorf("key Slave/gpus_revocable_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("percent", sAtt, subLabels)...).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			return nil
		},
		gauge("slave", "disk", "Current disk Resources in cluster.",
			defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["Slave/disk_percent"]
			if !ok {

				return fmt.Errorf("key Slave/disk_percent not found in map")

			}
			total, ok := m["Slave/disk_total"]
			if !ok {

				return fmt.Errorf("key Slave/disk_total not found in map")

			}
			used, ok := m["Slave/disk_used"]
			if !ok {

				return fmt.Errorf("key Slave/disk_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("percent", sAtt, subLabels)...).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			return nil
		},
		gauge("slave", "disk_revocable", "Current disk Resources in cluster.",
			defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["Slave/disk_revocable_percent"]
			if !ok {

				return fmt.Errorf("key Slave/disk_revocable_percent not found in map")

			}
			total, ok := m["Slave/disk_revocable_total"]
			if !ok {

				return fmt.Errorf("key Slave/disk_revocable_total not found in map")

			}
			used, ok := m["Slave/disk_revocable_used"]
			if !ok {

				return fmt.Errorf("key Slave/disk_revocable_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("percent", sAtt, subLabels)...).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			return nil
		},

		// Slave stats about uptime and connectivity
		gauge("slave", "registered", "1 if Slave is registered with master, 0 if not.",
			subLabels...): func(m MetricMap, c prometheus.Collector) error {
			registered, ok := m["Slave/registered"]
			if !ok {

				return fmt.Errorf("key Slave/registered not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("", sAtt, subLabels)...).Set(registered)
			return nil
		},
		gauge("slave", "uptime_seconds", "Number of seconds the Slave process is running.",
			subLabels...): func(m MetricMap, c prometheus.Collector) error {
			uptime, ok := m["Slave/uptime_secs"]
			if !ok {

				return fmt.Errorf("key Slave/uptime_seconds not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("", sAtt, subLabels)...).Set(uptime)
			return nil
		},
		counter("slave",
			"recovery_errors",
			"Total number of recovery errors", subLabels...): func(m MetricMap, c prometheus.Collector) error {
			errors, ok := m["Slave/recovery_errors"]
			if !ok {

				return fmt.Errorf("key Slave/recovery_errors not found in map")

			}
			c.(*SettableCounterVec).Set(errors, AddValueFromMap("", sAtt, subLabels)...)
			return nil
		},
		gauge("slave", "recovery_time_secs", "Agent recovery time in seconds",
			subLabels...): func(m MetricMap, c prometheus.Collector) error {
			age, ok := m["Slave/recovery_time_secs"]
			if !ok {

				return fmt.Errorf("key Slave/recovery_time_secs not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("", sAtt, subLabels)...).Set(age)
			return nil
		},
		gauge("slave", "executor_directory_max_allowed_age_secs",
			"Max allowed age of the Executor directory",
			subLabels...): func(m MetricMap, c prometheus.Collector) error {
			age, ok := m["Slave/executor_directory_max_allowed_age_secs"]
			if !ok {

				return fmt.Errorf("key Slave/executor_directory_max_allowed_age_secs not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("", sAtt, subLabels)...).Set(age)
			return nil
		},

		// Slave stats about frameworks and executors
		gauge("slave", "executor_state", "Current number of executors by State.",
			append([]string{"State"}, defaultLabels[1:]...)...): func(m MetricMap, c prometheus.Collector) error {
			registering, ok := m["Slave/executors_registering"]
			if !ok {

				return fmt.Errorf("key Slave/executors_registering not found in map")

			}
			running, ok := m["Slave/executors_running"]
			if !ok {

				return fmt.Errorf("key Slave/executors_running not found in map")

			}
			terminating, ok := m["Slave/executors_terminating"]
			if !ok {

				return fmt.Errorf("key Slave/executors_terminating not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("registering", sAtt, subLabels)...).Set(registering)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("running", sAtt, subLabels)...).Set(running)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("terminating", sAtt, subLabels)...).Set(terminating)
			return nil
		},
		gauge("slave", "frameworks_active", "Current number of active frameworks",
			subLabels...): func(m MetricMap, c prometheus.Collector) error {
			active, ok := m["Slave/frameworks_active"]
			if !ok {

				return fmt.Errorf("key Slave/frameworks_active not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("", sAtt, subLabels)...).Set(active)
			return nil
		},
		counter("slave",
			"executors_terminated",
			"Total number of Executor terminations.", subLabels...): func(m MetricMap, c prometheus.Collector) error {
			terminated, ok := m["Slave/executors_terminated"]
			if !ok {

				return fmt.Errorf("key Slave/executors_terminated not found in map")

			}
			c.(*SettableCounterVec).Set(terminated, AddValueFromMap("", sAtt, subLabels)...)
			return nil
		},
		counter("slave",
			"executors_preempted",
			"Total number of Executor preemptions.", subLabels...): func(m MetricMap, c prometheus.Collector) error {
			preempted, ok := m["Slave/executors_preempted"]
			if !ok {

				return fmt.Errorf("key Slave/executors_preempted not found in map")

			}
			c.(*SettableCounterVec).Set(preempted, AddValueFromMap("", sAtt, subLabels)...)
			return nil
		},

		// TODO bug
		// Slave stats about tasks
		counter("slave", "task_states_exit_total",
			"Total number of tasks processed by exit State.",
			append([]string{"State"}, defaultLabels[1:]...)...): func(m MetricMap, c prometheus.Collector) error {
			errored, ok := m["Slave/tasks_error"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_error not found in map")

			}
			failed, ok := m["Slave/tasks_failed"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_failed not found in map")

			}
			finished, ok := m["Slave/tasks_finished"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_finished not found in map")

			}
			gone, ok := m["Slave/tasks_gone"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_gone not found in map")

			}
			killed, ok := m["Slave/tasks_killed"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_killed not found in map")

			}

			lost, ok := m["Slave/tasks_lost"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_lost not found in map")

			}

			c.(*SettableCounterVec).Set(errored, AddValueFromMap("errored", sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(failed, AddValueFromMap("failed", sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(finished, AddValueFromMap("finished", sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(gone, AddValueFromMap("gone", sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(killed, AddValueFromMap("killed", sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(lost, AddValueFromMap("lost", sAtt, subLabels)...)

			return nil
		},
		counter("slave", "task_states_current", "Current number of tasks by State.",
			append([]string{"State"}, defaultLabels[1:]...)...): func(m MetricMap, c prometheus.Collector) error {
			running, ok := m["Slave/tasks_running"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_running not found in map")

			}
			staging, ok := m["Slave/tasks_staging"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_staging not found in map")

			}
			starting, ok := m["Slave/tasks_starting"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_starting not found in map")

			}
			killing, ok := m["Slave/tasks_killing"]
			if !ok {

				return fmt.Errorf("key Slave/tasks_killing not found in map")

			}

			c.(*SettableCounterVec).Set(killing, AddValueFromMap("killing", sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(running, AddValueFromMap("running", sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(staging, AddValueFromMap("staging", sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(starting, AddValueFromMap("starting", sAtt, subLabels)...)

			return nil
		},

		// TODO ...
		counter("slave", "task_state_counts_by_source_reason", "Number of Task states by source and reason",
			append([]string{"State", "source", "reason"}, defaultLabels[1:]...)...): func(m MetricMap, c prometheus.Collector) error {
			re, err := regexp.Compile("Slave/task_(.*?)/source_(.*?)/reason_(.*?)$")
			if err != nil {
				log.WithFields(log.Fields{
					"regex":  "Slave/task_(.*?)/source_(.*?)/reason_(.*?)$",
					"Metric": "slave_task_state_counts_by_source_reason",
					"error":  err,
				}).Error("could not compile regex")
				return fmt.Errorf("could not compile slave_task_state_counts_by_source_reason regex: %s", err)
			}
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				state := matches[1]
				source := matches[2]
				reason := matches[3]
				c.(*SettableCounterVec).Set(value, AddValuesFromMap([]string{state, source, reason}, sAtt, subLabels)...)
			}
			return nil
		},

		// Slave stats about messages
		counter("slave", "messages_outcomes_total",
			"Total number of messages by outcome of operation",
			append([]string{"type", "outcome"}, defaultLabels[1:]...)...): func(m MetricMap, c prometheus.Collector) error {

			frameworkMessagesValid, ok := m["Slave/valid_framework_messages"]
			if !ok {

				return fmt.Errorf("key Slave/valid_framework_messages not found in map")

			}
			frameworkMessagesInvalid, ok := m["Slave/invalid_framework_messages"]
			if !ok {

				return fmt.Errorf("key Slave/invalid_framework_messages not found in map")

			}
			statusUpdateValid, ok := m["Slave/valid_status_updates"]
			if !ok {

				return fmt.Errorf("key Slave/valid_status_updates not found in map")

			}
			statusUpdateInvalid, ok := m["Slave/invalid_status_updates"]
			if !ok {

				return fmt.Errorf("key Slave/invalid_status_updates not found in map")

			}
			c.(*SettableCounterVec).Set(frameworkMessagesValid, AddValuesFromMap([]string{"Framework", "valid"}, sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(frameworkMessagesInvalid, AddValuesFromMap([]string{"Framework", "invalid"}, sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(statusUpdateValid, AddValuesFromMap([]string{"Status", "valid"}, sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(statusUpdateInvalid, AddValuesFromMap([]string{"Status", "invalid"}, sAtt, subLabels)...)

			return nil
		},

		// GC information
		gauge("slave", "gc_path_removals_pending",
			"Number of sandbox paths that are currently pending agent garbage collection",
			subLabels...): func(m MetricMap, c prometheus.Collector) error {
			pending, ok := m["gc/path_removals_pending"]
			if !ok {

				return fmt.Errorf("key gc/path_removals_pending not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("", sAtt, subLabels)...).Set(pending)
			return nil
		},
		counter("slave", "gc_path_removals_outcome",
			"Number of sandbox paths the agent removed",
			append([]string{"outcome"}, defaultLabels[1:]...)...): func(m MetricMap, c prometheus.Collector) error {

			succeeded, ok := m["gc/path_removals_succeeded"]
			if !ok {

				return fmt.Errorf("key gc/path_removals_succeeded not found in map")

			}
			failed, ok := m["gc/path_removals_failed"]
			if !ok {

				return fmt.Errorf("key gc/path_removals_failed not found in map")

			}
			c.(*SettableCounterVec).Set(succeeded, AddValuesFromMap([]string{"success"}, sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(failed, AddValuesFromMap([]string{"failed"}, sAtt, subLabels)...)

			return nil
		},

		// Container / Containerizer information
		counter("slave",
			"container_launch_errors",
			"Total number of container launch errors", subLabels...): func(m MetricMap, c prometheus.Collector) error {
			errors, ok := m["Slave/container_launch_errors"]
			if !ok {

				return fmt.Errorf("key Slave/container_launch_errors not found in map")

			}
			c.(*SettableCounterVec).Set(errors, AddValueFromMap("", sAtt, subLabels)...)
			return nil
		},
		counter("slave",
			"containerizer_filesystem_containers_new_rootfs",
			"Number of containers changing root filesystem", subLabels...): func(m MetricMap, c prometheus.Collector) error {
			newRootfs, ok := m["containerizer/mesos/filesystem/containers_new_rootfs"]
			if !ok {

				return fmt.Errorf("key containerizer/mesos/filesystem/containers_new_rootfs not found in map")

			}
			c.(*SettableCounterVec).Set(newRootfs, AddValueFromMap("", sAtt, subLabels)...)
			return nil
		},
		counter("slave",
			"containerizer_provisioner_bind_remove_rootfs_errors",
			"Number of errors from the containerizer attempting to bind the rootfs", subLabels...): func(m MetricMap, c prometheus.Collector) error {
			errors, ok := m["containerizer/mesos/provisioner/bind/remove_rootfs_errors"]
			if !ok {

				return fmt.Errorf("key containerizer/mesos/provisioner/bind/remove_rootfs_errors not found in map")

			}
			c.(*SettableCounterVec).Set(errors, AddValueFromMap("", sAtt, subLabels)...)
			return nil
		},
		counter("slave",
			"containerizer_provisioner_remove_container_errors",
			"Number of errors from the containerizer attempting to remove a container", subLabels...): func(m MetricMap, c prometheus.Collector) error {
			errors, ok := m["containerizer/mesos/provisioner/remove_container_errors"]
			if !ok {

				return fmt.Errorf("key containerizer/mesos/provisioner/remove_container_errors not found in map")

			}
			c.(*SettableCounterVec).Set(errors, AddValueFromMap("", sAtt, subLabels)...)
			return nil
		},
		counter("slave",
			"containerizer_container_destroy_errors",
			"Number of containers destroyed due to launch errors", subLabels...): func(m MetricMap, c prometheus.Collector) error {
			errors, ok := m["containerizer/mesos/container_destroy_errors"]
			if !ok {

				return fmt.Errorf("key containerizer/mesos/container_destroy_errors not found in map")

			}
			c.(*SettableCounterVec).Set(errors, AddValueFromMap("", sAtt, subLabels)...)
			return nil
		},
		counter("slave", "containerizer_fetcher_task_fetches",
			"Total number of containerizer fetcher tasks by outcome",
			append([]string{"outcome"}, defaultLabels[1:]...)...): func(m MetricMap, c prometheus.Collector) error {

			succeeded, ok := m["containerizer/fetcher/task_fetches_succeeded"]
			if !ok {

				return fmt.Errorf("key containerizer/fetcher/task_fetches_succeeded not found in map")

			}
			failed, ok := m["containerizer/fetcher/task_fetches_failed"]
			if !ok {

				return fmt.Errorf("key containerizer/fetcher/task_fetches_failed not found in map")

			}
			c.(*SettableCounterVec).Set(succeeded, AddValuesFromMap([]string{"success"}, sAtt, subLabels)...)
			c.(*SettableCounterVec).Set(failed, AddValuesFromMap([]string{"failed"}, sAtt, subLabels)...)

			return nil
		},
		gauge("slave", "containerizer_fetcher_cache_size", "Containerizer fetcher cache sizes in bytes", defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			total, ok := m["containerizer/fetcher/cache_size_total_bytes"]
			if !ok {

				return fmt.Errorf("key containerizer/fetcher/cache_size_total_bytes not found in map")

			}
			used, ok := m["containerizer/fetcher/cache_size_used_bytes"]
			if !ok {

				return fmt.Errorf("key containerizer/fetcher/cache_size_used_bytes not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(used)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(total - used)
			return nil
		},
		gauge("slave", "containerizer_xfs_project_ids", "Number of project IDs available for the XFS disk isolator", defaultLabels...): func(m MetricMap, c prometheus.Collector) error {
			total, ok := m["containerizer/mesos/disk/project_ids_total"]
			if !ok {

				return fmt.Errorf("key containerizer/mesos/disk/project_ids_total not found in map")

			}
			free, ok := m["containerizer/mesos/disk/project_ids_free"]
			if !ok {

				return fmt.Errorf("key containerizer/mesos/disk/project_ids_free not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("total", sAtt, subLabels)...).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("used", sAtt, subLabels)...).Set(total - free)
			c.(*prometheus.GaugeVec).WithLabelValues(AddValueFromMap("free", sAtt, subLabels)...).Set(free)
			return nil
		},

		// END
	}
	return newMetricCollector(httpClient, metrics)
}
