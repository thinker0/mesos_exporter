package mesos

import (
	"fmt"
	"regexp"

	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

func NewMasterCollector(httpClient *HttpClient) prometheus.Collector {
	hostname := httpClient.Hostname
	metrics := map[prometheus.Collector]func(MetricMap, prometheus.Collector) error{
		// CPU/Disk/Mem Resources in free/used
		gauge("master", "cpus", "Current CPU Resources in cluster.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["master/cpus_percent"]
			if !ok {

				return fmt.Errorf("key master/cpus_percent not found in map")

			}
			total, ok := m["master/cpus_total"]
			if !ok {

				return fmt.Errorf("key master/cpus_total not found in map")

			}
			used, ok := m["master/cpus_used"]
			if !ok {

				return fmt.Errorf("key master/cpus_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("percent", hostname).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("free", hostname).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues("used", hostname).Set(used)
			return nil
		},
		gauge("master", "cpus_revocable", "Current revocable CPU Resources in cluster.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["master/cpus_revocable_percent"]
			if !ok {

				return fmt.Errorf("key master/cpus_revocable_percent not found in map")

			}
			total, ok := m["master/cpus_revocable_total"]
			if !ok {

				return fmt.Errorf("key master/cpus_revocable_total not found in map")

			}
			used, ok := m["master/cpus_revocable_used"]
			if !ok {

				return fmt.Errorf("key master/cpus_revocable_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("percent", hostname).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("free", hostname).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues("used", hostname).Set(used)
			return nil
		},
		gauge("master", "gpus", "Current GPU Resources in cluster.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["master/gpus_percent"]
			if !ok {

				return fmt.Errorf("key master/gpus_percent not found in map")

			}
			total, ok := m["master/gpus_total"]
			if !ok {

				return fmt.Errorf("key master/gpus_total not found in map")

			}
			used, ok := m["master/gpus_used"]
			if !ok {

				return fmt.Errorf("key master/gpus_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("percent", hostname).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("free", hostname).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues("used", hostname).Set(used)
			return nil
		},
		gauge("master", "gpus_revocable", "Current revocable GPU Resources in cluster.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["master/gpus_revocable_percent"]
			if !ok {

				return fmt.Errorf("key master/gpus_revocable_percent not found in map")

			}
			total, ok := m["master/gpus_revocable_total"]
			if !ok {

				return fmt.Errorf("key master/gpus_revocable_total not found in map")

			}
			used, ok := m["master/gpus_revocable_used"]
			if !ok {

				return fmt.Errorf("key master/gpus_revocable_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("percent", hostname).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("free", hostname).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues("used", hostname).Set(used)
			return nil
		},
		gauge("master", "mem", "Current memory Resources in cluster.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["master/mem_percent"]
			if !ok {

				return fmt.Errorf("key master/mem_percent not found in map")

			}
			total, ok := m["master/mem_total"]
			if !ok {

				return fmt.Errorf("key master/mem_total not found in map")

			}
			used, ok := m["master/mem_used"]
			if !ok {

				return fmt.Errorf("key master/mem_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("percent", hostname).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("free", hostname).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues("used", hostname).Set(used)
			return nil
		},
		gauge("master", "mem_revocable", "Current revocable memory Resources in cluster.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["master/mem_revocable_percent"]
			if !ok {

				return fmt.Errorf("key master/mem_revocable_percent not found in map")

			}
			total, ok := m["master/mem_revocable_total"]
			if !ok {

				return fmt.Errorf("key master/mem_revocable_total not found in map")

			}
			used, ok := m["master/mem_revocable_used"]
			if !ok {

				return fmt.Errorf("key master/mem_revocable_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("percent", hostname).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("free", hostname).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues("used", hostname).Set(used)
			return nil
		},
		gauge("master", "disk", "Current disk Resources in cluster.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["master/disk_percent"]
			if !ok {

				return fmt.Errorf("key master/disk_percent not found in map")

			}
			total, ok := m["master/disk_total"]
			if !ok {

				return fmt.Errorf("key master/disk_total not found in map")

			}
			used, ok := m["master/disk_used"]
			if !ok {

				return fmt.Errorf("key master/disk_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("percent", hostname).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("free", hostname).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues("used", hostname).Set(used)
			return nil
		},
		gauge("master", "disk_revocable", "Current disk Resources in cluster.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			percent, ok := m["master/disk_revocable_percent"]
			if !ok {

				return fmt.Errorf("key master/disk_revocable_percent not found in map")

			}
			total, ok := m["master/disk_revocable_total"]
			if !ok {

				return fmt.Errorf("key master/disk_revocable_total not found in map")

			}
			used, ok := m["master/disk_revocable_used"]
			if !ok {

				return fmt.Errorf("key master/disk_revocable_used not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("percent", hostname).Set(percent)
			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("free", hostname).Set(total - used)
			c.(*prometheus.GaugeVec).WithLabelValues("used", hostname).Set(used)
			return nil
		},
		// Master stats about uptime and election State
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "master",
			Name:      "elected",
			Help:      "1 if master is elected leader, 0 if not",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			elected, ok := m["master/elected"]
			if !ok {

				return fmt.Errorf("key master/elected not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(elected)
			return nil
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "master",
			Name:      "uptime_seconds",
			Help:      "Number of seconds the master process is running.",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			uptime, ok := m["master/uptime_secs"]
			if !ok {

				return fmt.Errorf("key master/uptime_secs not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(uptime)
			return nil
		},
		// Master stats about agents
		counter("master", "slave_registration_events_total", "Total number of registration events on this master since it booted.", "event", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			registrations, ok := m["master/slave_registrations"]
			if !ok {

				return fmt.Errorf("key master/slave_registrations not found in map")

			}
			reregistrations, ok := m["master/slave_reregistrations"]
			if !ok {

				return fmt.Errorf("key master/slave_reregistrations not found in map")

			}
			c.(*SettableCounterVec).Set(registrations, "register", hostname)
			c.(*SettableCounterVec).Set(reregistrations, "reregister", hostname)
			return nil
		},

		counter("master", "recovery_slave_removal_events_total", "Total number of recovery removal events on this master since it booted.", "event", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			removals, ok := m["master/recovery_slave_removals"]
			if !ok {

				return fmt.Errorf("key master/recovery_slave_removals not found in map")

			}
			c.(*SettableCounterVec).Set(removals, "removal", hostname)
			return nil
		},

		counter("master", "slave_removal_events_total", "Total number of removal events on this master since it booted.", "event", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			scheduled, ok := m["master/slave_shutdowns_scheduled"]
			if !ok {

				return fmt.Errorf("key master/slave_shutdowns_scheduled not found in map")

			}
			canceled, ok := m["master/slave_shutdowns_canceled"]
			if !ok {

				return fmt.Errorf("key master/slave_shutdowns_canceled not found in map")

			}
			completed, ok := m["master/slave_shutdowns_completed"]
			if !ok {

				return fmt.Errorf("key master/slave_shutdowns_completed not found in map")

			}
			removals, ok := m["master/slave_removals"]
			if !ok {

				return fmt.Errorf("key master/slave_removals not found in map")

			}
			c.(*SettableCounterVec).Set(scheduled, "scheduled", hostname)
			c.(*SettableCounterVec).Set(canceled, "canceled", hostname)
			c.(*SettableCounterVec).Set(completed, "completed", hostname)
			// set this explicitly to be more obvious
			died := removals - completed
			c.(*SettableCounterVec).Set(died, "died", hostname)
			return nil
		},
		counter("master", "slave_removal_events_reasons", "Total number of Slave removal events by reason on this master since it booted.", "reason", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, err := regexp.Compile("master/slave_removals/reason_(.*?)$")
			if err != nil {
				log.WithFields(log.Fields{
					"regex":  "master/slave_removals/reason_(.*?)$",
					"Metric": "master_slave_removal_events_reasons",
					"error":  err,
				}).Error("could not compile regex")
				return fmt.Errorf("could not compile slave_removal_events_reasons regex: %s", err)
			}
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 2 {
					continue
				}
				reason := matches[1]
				c.(*SettableCounterVec).Set(value, reason, hostname)
			}
			return nil
		},
		counter("master", "slave_unreachable_events_total", "Total number of Slave unreachable events on this master since it booted.", "event", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			canceled, ok := m["master/slave_unreachable_canceled"]
			if !ok {

				return fmt.Errorf("key master/slave_unreachable_canceled not found in map")

			}
			completed, ok := m["master/slave_unreachable_completed"]
			if !ok {

				return fmt.Errorf("key master/slave_unreachable_completed not found in map")

			}
			scheduled, ok := m["master/slave_unreachable_scheduled"]
			if !ok {

				return fmt.Errorf("key master/slave_unreachable_scheduled not found in map")

			}
			c.(*SettableCounterVec).Set(canceled, "canceled", hostname)
			c.(*SettableCounterVec).Set(completed, "completed", hostname)
			c.(*SettableCounterVec).Set(scheduled, "scheduled", hostname)
			return nil
		},

		gauge("master", "slaves_state", "Current number of slaves known to the master per connection and registration State.", "State", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			active, ok := m["master/slaves_active"]
			if !ok {

				return fmt.Errorf("key master/slaves_active not found in map")

			}
			inactive, ok := m["master/slaves_inactive"]
			if !ok {

				return fmt.Errorf("key master/slaves_inactive not found in map")

			}
			disconnected, ok := m["master/slaves_disconnected"]
			if !ok {

				return fmt.Errorf("key master/slaves_disconnected not found in map")

			}
			unreachable, ok := m["master/slaves_unreachable"]
			if !ok {

				return fmt.Errorf("key master/slaves_unreachable not found in map")

			}

			// FIXME: Make sure those assumptions are right
			// Every "active" node is connected to the master
			c.(*prometheus.GaugeVec).WithLabelValues("connected_active", hostname).Set(active)
			// Every "inactive" node is connected but node sending offers
			c.(*prometheus.GaugeVec).WithLabelValues("connected_inactive", hostname).Set(inactive)
			// Every "disconnected" node is "inactive"
			c.(*prometheus.GaugeVec).WithLabelValues("disconnected_inactive", hostname).Set(disconnected)
			// Every "connected" node is either active or inactive
			c.(*prometheus.GaugeVec).WithLabelValues("unreachable", hostname).Set(unreachable)
			return nil
		},

		// Master stats about frameworks
		gauge("master", "frameworks_state", "Current number of frames known to the master per connection and registration State.", "State", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			active, ok := m["master/frameworks_active"]
			if !ok {

				return fmt.Errorf("key master/frameworks_active not found in map")

			}
			inactive, ok := m["master/frameworks_inactive"]
			if !ok {

				return fmt.Errorf("key master/frameworks_inactive not found in map")

			}
			disconnected, ok := m["master/frameworks_disconnected"]
			if !ok {

				return fmt.Errorf("key master/frameworks_disconnected not found in map")

			}
			// FIXME: Make sure those assumptions are right
			// Every "active" Framework is connected to the master
			c.(*prometheus.GaugeVec).WithLabelValues("connected_active", hostname).Set(active)
			// Every "inactive" Framework is connected but Framework sending offers
			c.(*prometheus.GaugeVec).WithLabelValues("connected_inactive", hostname).Set(inactive)
			// Every "disconnected" Framework is "inactive"
			c.(*prometheus.GaugeVec).WithLabelValues("disconnected_inactive", hostname).Set(disconnected)
			// Every "connected" Framework is either active or inactive
			return nil
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "master",
			Name:      "offers_pending",
			Help:      "Current number of offers made by the master which aren't yet accepted or declined by frameworks.",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			offers, ok := m["master/outstanding_offers"]
			if !ok {

				return fmt.Errorf("key master/outstanding_offers not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(offers)
			return nil
		},

		// Master stats about tasks
		counter("master", "task_states_exit_total", "Total number of tasks processed by exit State.", "State", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			dropped, ok := m["master/tasks_dropped"]
			if !ok {

				return fmt.Errorf("key master/tasks_dropped not found in map")

			}
			errored, ok := m["master/tasks_error"]
			if !ok {

				return fmt.Errorf("key master/tasks_error not found in map")

			}
			failed, ok := m["master/tasks_failed"]
			if !ok {

				return fmt.Errorf("key master/tasks_failed not found in map")

			}
			finished, ok := m["master/tasks_finished"]
			if !ok {

				return fmt.Errorf("key master/tasks_finished not found in map")

			}
			gone, ok := m["master/tasks_gone"]
			if !ok {

				return fmt.Errorf("key master/tasks_gone not found in map")

			}
			goneByOperator, ok := m["master/tasks_gone_by_operator"]
			if !ok {

				return fmt.Errorf("key master/tasks_gone_by_operator not found in map")

			}
			killed, ok := m["master/tasks_killed"]
			if !ok {

				return fmt.Errorf("key master/tasks_killed not found in map")

			}
			lost, ok := m["master/tasks_lost"]
			if !ok {

				return fmt.Errorf("key master/tasks_lost not found in map")

			}

			c.(*SettableCounterVec).Set(dropped, "dropped", hostname)
			c.(*SettableCounterVec).Set(errored, "errored", hostname)
			c.(*SettableCounterVec).Set(failed, "failed", hostname)
			c.(*SettableCounterVec).Set(finished, "finished", hostname)
			c.(*SettableCounterVec).Set(goneByOperator, "gone_by_operator", hostname)
			c.(*SettableCounterVec).Set(gone, "gone", hostname)
			c.(*SettableCounterVec).Set(killed, "killed", hostname)
			c.(*SettableCounterVec).Set(lost, "lost", hostname)

			return nil
		},

		gauge("master", "task_states_current", "Current number of tasks by State.", "State", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			running, ok := m["master/tasks_running"]
			if !ok {

				return fmt.Errorf("key master/tasks_running not found in map")

			}
			staging, ok := m["master/tasks_staging"]
			if !ok {

				return fmt.Errorf("key master/tasks_staging not found in map")

			}
			starting, ok := m["master/tasks_starting"]
			if !ok {

				return fmt.Errorf("key master/tasks_starting not found in map")

			}
			unreachable, ok := m["master/tasks_unreachable"]
			if !ok {

				return fmt.Errorf("key master/tasks_unreachable not found in map")

			}

			killing, ok := m["master/tasks_killing"]
			if !ok {

				return fmt.Errorf("key master/tasks_killing not found in map")

			}

			c.(*prometheus.GaugeVec).WithLabelValues("killing", hostname).Set(killing)
			c.(*prometheus.GaugeVec).WithLabelValues("running", hostname).Set(running)
			c.(*prometheus.GaugeVec).WithLabelValues("staging", hostname).Set(staging)
			c.(*prometheus.GaugeVec).WithLabelValues("starting", hostname).Set(starting)
			c.(*prometheus.GaugeVec).WithLabelValues("unreachable", hostname).Set(unreachable)

			return nil
		},

		counter("master", "task_state_counts_by_source_reason", "Number of Task states by source and reason", "State", "source", "reason", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, err := regexp.Compile("master/task_(.*?)/source_(.*?)/reason_(.*?)$")
			if err != nil {
				log.WithFields(log.Fields{
					"regex":  "master/task_(.*?)/source_(.*?)/reason_(.*?)$",
					"Metric": "master_task_state_counts_by_source_reason",
					"error":  err,
				}).Error("could not compile regex")
				return fmt.Errorf("could not compile task_state_counts regex: %s", err)
			}
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				state := matches[1]
				source := matches[2]
				reason := matches[3]
				c.(*SettableCounterVec).Set(value, state, source, reason, hostname)
			}
			return nil
		},

		// Master stats about messages
		counter("master", "messages", "Number of messages by the master by State", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			droppedMessages, ok := m["master/dropped_messages"]
			if !ok {

				return fmt.Errorf("key master/dropped_messages not found in map")

			}
			authenticateMessages, ok := m["master/messages_authenticate"]
			if !ok {

				return fmt.Errorf("key master/messages_authenticate not found in map")

			}
			deactivateFrameworkMessages, ok := m["master/messages_deactivate_framework"]
			if !ok {

				return fmt.Errorf("key master/messages_deactivate_framework not found in map")

			}
			declineOfferMessages, ok := m["master/messages_decline_offers"]
			if !ok {

				return fmt.Errorf("key master/messages_decline_offers not found in map")

			}
			executorToFrameworkMessages, ok := m["master/messages_executor_to_framework"]
			if !ok {

				return fmt.Errorf("key master/messages_executor_to_framework not found in map")

			}
			exitedExecutor, ok := m["master/messages_exited_executor"]
			if !ok {

				return fmt.Errorf("key master/messages_exited_executor not found in map")

			}
			frameworkToExecutor, ok := m["master/messages_framework_to_executor"]
			if !ok {

				return fmt.Errorf("key master/messages_framework_to_executor not found in map")

			}
			killTask, ok := m["master/messages_kill_task"]
			if !ok {

				return fmt.Errorf("key master/messages_kill_task not found in map")

			}
			launchTasks, ok := m["master/messages_launch_tasks"]
			if !ok {

				return fmt.Errorf("key master/messages_launch_tasks not found in map")

			}
			reconcileTasks, ok := m["master/messages_reconcile_tasks"]
			if !ok {

				return fmt.Errorf("key master/messages_reconcile_tasks not found in map")

			}
			registerFramework, ok := m["master/messages_register_framework"]
			if !ok {

				return fmt.Errorf("key master/messages_register_framework not found in map")

			}
			registerSlave, ok := m["master/messages_register_slave"]
			if !ok {

				return fmt.Errorf("key master/messages_register_slave not found in map")

			}
			reregisterFramework, ok := m["master/messages_reregister_framework"]
			if !ok {

				return fmt.Errorf("key master/messages_reregister_framework not found in map")

			}
			reregisterSlave, ok := m["master/messages_reregister_slave"]
			if !ok {

				return fmt.Errorf("key master/messages_reregister_slave not found in map")

			}
			resourceRequest, ok := m["master/messages_resource_request"]
			if !ok {

				return fmt.Errorf("key master/messages_resource_request not found in map")

			}
			reviveOffers, ok := m["master/messages_revive_offers"]
			if !ok {

				return fmt.Errorf("key master/messages_revive_offers not found in map")

			}
			statusUpdate, ok := m["master/messages_status_update"]
			if !ok {

				return fmt.Errorf("key master/messages_status_update not found in map")

			}
			statusUpdateAck, ok := m["master/messages_status_update_acknowledgement"]
			if !ok {

				return fmt.Errorf("key master/messages_status_update_acknowledgement not found in map")

			}
			suppressOffers, ok := m["master/messages_suppress_offers"]
			if !ok {

				return fmt.Errorf("key master/messages_suppress_offers not found in map")

			}
			unregisterFramework, ok := m["master/messages_unregister_framework"]
			if !ok {

				return fmt.Errorf("key master/messages_unregister_framework not found in map")

			}
			unregisterSlave, ok := m["master/messages_unregister_slave"]
			if !ok {

				return fmt.Errorf("key master/messages_unregister_slave not found in map")

			}
			updateSlave, ok := m["master/messages_update_slave"]
			if !ok {

				return fmt.Errorf("key master/messages_update_slave not found in map")

			}

			c.(*SettableCounterVec).Set(authenticateMessages, "authenticate_messages", hostname)
			c.(*SettableCounterVec).Set(droppedMessages, "dropped_messages", hostname)
			c.(*SettableCounterVec).Set(deactivateFrameworkMessages, "deactivate_framework", hostname)
			c.(*SettableCounterVec).Set(declineOfferMessages, "decline_offers", hostname)
			c.(*SettableCounterVec).Set(executorToFrameworkMessages, "executor_to_framework", hostname)
			c.(*SettableCounterVec).Set(exitedExecutor, "exited_executor", hostname)
			c.(*SettableCounterVec).Set(frameworkToExecutor, "framework_to_executor", hostname)
			c.(*SettableCounterVec).Set(killTask, "kill_task", hostname)
			c.(*SettableCounterVec).Set(launchTasks, "launch_tasks", hostname)
			c.(*SettableCounterVec).Set(reconcileTasks, "reconcile_tasks", hostname)
			c.(*SettableCounterVec).Set(registerFramework, "register_framework", hostname)
			c.(*SettableCounterVec).Set(registerSlave, "register_slave", hostname)
			c.(*SettableCounterVec).Set(reregisterFramework, "reregister_framework", hostname)
			c.(*SettableCounterVec).Set(reregisterSlave, "reregister_slave", hostname)
			c.(*SettableCounterVec).Set(resourceRequest, "resource_request", hostname)
			c.(*SettableCounterVec).Set(reviveOffers, "revive_offers", hostname)
			c.(*SettableCounterVec).Set(statusUpdate, "status_update", hostname)
			c.(*SettableCounterVec).Set(statusUpdateAck, "status_update_acknowledgement", hostname)
			c.(*SettableCounterVec).Set(suppressOffers, "suppress_offers", hostname)
			c.(*SettableCounterVec).Set(unregisterFramework, "unregister_framework", hostname)
			c.(*SettableCounterVec).Set(unregisterSlave, "unregister_slave", hostname)
			c.(*SettableCounterVec).Set(updateSlave, "update_slave", hostname)
			return nil
		},

		counter("master", "messages_outcomes_total",
			"Total number of messages by outcome of operation and direction.",
			"source", "destination", "type", "outcome", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			frameworkToExecutorValid, ok := m["master/valid_framework_to_executor_messages"]
			if !ok {

				return fmt.Errorf("key master/valid_framework_to_executor_messages not found in map")

			}
			frameworkToExecutorInvalid, ok := m["master/invalid_framework_to_executor_messages"]
			if !ok {

				return fmt.Errorf("key master/invalid_framework_to_executor_messages not found in map")

			}
			executorToFrameworkValid, ok := m["master/valid_executor_to_framework_messages"]
			if !ok {

				return fmt.Errorf("key master/valid_executor_to_framework_messages not found in map")

			}
			executorToFrameworkInvalid, ok := m["master/invalid_executor_to_framework_messages"]
			if !ok {

				return fmt.Errorf("key master/invalid_executor_to_framework_messages not found in map")

			}

			// Status updates are sent from Framework?(FIXME) to Slave
			// Status update acks are sent from Slave to Framework?
			statusUpdateAckValid, ok := m["master/valid_status_update_acknowledgements"]
			if !ok {

				return fmt.Errorf("key master/valid_status_update_acknowledgements not found in map")

			}
			statusUpdateAckInvalid, ok := m["master/invalid_status_update_acknowledgements"]
			if !ok {

				return fmt.Errorf("key master/invalid_status_update_acknowledgements not found in map")

			}
			statusUpdateValid, ok := m["master/valid_status_updates"]
			if !ok {

				return fmt.Errorf("key master/valid_status_updates not found in map")

			}
			statusUpdateInvalid, ok := m["master/invalid_status_updates"]
			if !ok {

				return fmt.Errorf("key master/invalid_status_updates not found in map")

			}
			c.(*SettableCounterVec).Set(frameworkToExecutorValid, "Framework", "Executor", "", "valid", hostname)
			c.(*SettableCounterVec).Set(frameworkToExecutorInvalid, "Framework", "Executor", "", "invalid", hostname)

			c.(*SettableCounterVec).Set(executorToFrameworkValid, "Executor", "Framework", "", "valid", hostname)
			c.(*SettableCounterVec).Set(executorToFrameworkInvalid, "Executor", "Framework", "", "invalid", hostname)

			// We consider a ack message simply as a message from Slave to Framework
			c.(*SettableCounterVec).Set(statusUpdateValid, "Framework", "Slave", "status_update", "valid", hostname)
			c.(*SettableCounterVec).Set(statusUpdateInvalid, "Framework", "Slave", "status_update", "invalid", hostname)
			c.(*SettableCounterVec).Set(statusUpdateAckValid, "Slave", "Framework", "status_update", "valid", hostname)
			c.(*SettableCounterVec).Set(statusUpdateAckInvalid, "Slave", "Framework", "status_update", "invalid", hostname)
			return nil
		},
		// Master stats about events
		gauge("master", "event_queue_length", "Current number of elements in event queue by type", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			dispatches, ok := m["master/event_queue_dispatches"]
			if !ok {

				return fmt.Errorf("key master/event_queue_dispatches not found in map")

			}
			httpRequests, ok := m["master/event_queue_http_requests"]
			if !ok {

				return fmt.Errorf("key master/event_queue_http_requests not found in map")

			}
			messages, ok := m["master/event_queue_messages"]
			if !ok {

				return fmt.Errorf("key master/event_queue_messages not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("message", hostname).Set(messages)
			c.(*prometheus.GaugeVec).WithLabelValues("http_request", hostname).Set(httpRequests)
			c.(*prometheus.GaugeVec).WithLabelValues("dispatches", hostname).Set(dispatches)
			return nil
		},

		// Master stats about allocations
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "master",
			Name:      "allocator_event_queue_dispatches",
			Help:      "Number of dispatch events in the allocator event queue.",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			count, ok := m["allocator/event_queue_dispatches"]
			if !ok {

				return fmt.Errorf("key allocator/event_queue_dispatches not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(count)
			return nil
		},

		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "master",
			Name:      "allocation_run_ms_count",
			Help:      "Number of allocation algorithm time measurements in the window",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			count, ok := m["allocator/mesos/allocation_runs"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_runs not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(count)
			return nil
		},

		gauge("master", "allocation_run_ms", "Time spent in allocation algorithm in ms.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			mean, ok := m["allocator/mesos/allocation_run_ms"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms not found in map")

			}
			min, ok := m["allocator/mesos/allocation_run_ms/min"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms/min not found in map")

			}
			max, ok := m["allocator/mesos/allocation_run_ms/max"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms/max not found in map")

			}
			p50, ok := m["allocator/mesos/allocation_run_ms/p50"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms/p50 not found in map")

			}
			p90, ok := m["allocator/mesos/allocation_run_ms/p90"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms/p90 not found in map")

			}
			p95, ok := m["allocator/mesos/allocation_run_ms/p95"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms/p95 not found in map")

			}
			p99, ok := m["allocator/mesos/allocation_run_ms/p99"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms/p99 not found in map")

			}
			p999, ok := m["allocator/mesos/allocation_run_ms/p999"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms/p999 not found in map")

			}
			p9999, ok := m["allocator/mesos/allocation_run_ms/p9999"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_ms/p9999 not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("mean", hostname).Set(mean)
			c.(*prometheus.GaugeVec).WithLabelValues("min", hostname).Set(min)
			c.(*prometheus.GaugeVec).WithLabelValues("max", hostname).Set(max)
			c.(*prometheus.GaugeVec).WithLabelValues("p50", hostname).Set(p50)
			c.(*prometheus.GaugeVec).WithLabelValues("p90", hostname).Set(p90)
			c.(*prometheus.GaugeVec).WithLabelValues("p95", hostname).Set(p95)
			c.(*prometheus.GaugeVec).WithLabelValues("p99", hostname).Set(p99)
			c.(*prometheus.GaugeVec).WithLabelValues("p999", hostname).Set(p999)
			c.(*prometheus.GaugeVec).WithLabelValues("p9999", hostname).Set(p9999)
			return nil
		},

		counter("master", "allocation_runs", "Number of times the allocation alorithm has run", "event", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			runs, ok := m["allocator/mesos/allocation_runs"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_runs not found in map")

			}
			c.(*SettableCounterVec).Set(runs, "allocation", hostname)
			return nil
		},

		counter("master", "allocation_run_latency_ms_count", "Number of allocation batch latency measurements", "event", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			count, ok := m["allocator/mesos/allocation_run_latency_ms/count"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/count not found in map")

			}
			c.(*SettableCounterVec).Set(count, "allocation", hostname)
			return nil
		},

		gauge("master", "allocation_run_latency_ms", "Allocation batch latency in ms.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			mean, ok := m["allocator/mesos/allocation_run_latency_ms"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms not found in map")

			}
			min, ok := m["allocator/mesos/allocation_run_latency_ms/min"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/min not found in map")

			}
			max, ok := m["allocator/mesos/allocation_run_latency_ms/max"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/max not found in map")

			}
			p50, ok := m["allocator/mesos/allocation_run_latency_ms/p50"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/p50 not found in map")

			}
			p90, ok := m["allocator/mesos/allocation_run_latency_ms/p90"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/p90 not found in map")

			}
			p95, ok := m["allocator/mesos/allocation_run_latency_ms/p95"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/p95 not found in map")

			}
			p99, ok := m["allocator/mesos/allocation_run_latency_ms/p99"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/p99 not found in map")

			}
			p999, ok := m["allocator/mesos/allocation_run_latency_ms/p999"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/p999 not found in map")

			}
			p9999, ok := m["allocator/mesos/allocation_run_latency_ms/p9999"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/allocation_run_latency_ms/p9999 not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues("mean", hostname).Set(mean)
			c.(*prometheus.GaugeVec).WithLabelValues("min", hostname).Set(min)
			c.(*prometheus.GaugeVec).WithLabelValues("max", hostname).Set(max)
			c.(*prometheus.GaugeVec).WithLabelValues("p50", hostname).Set(p50)
			c.(*prometheus.GaugeVec).WithLabelValues("p90", hostname).Set(p90)
			c.(*prometheus.GaugeVec).WithLabelValues("p95", hostname).Set(p95)
			c.(*prometheus.GaugeVec).WithLabelValues("p99", hostname).Set(p99)
			c.(*prometheus.GaugeVec).WithLabelValues("p999", hostname).Set(p999)
			c.(*prometheus.GaugeVec).WithLabelValues("p9999", hostname).Set(p9999)
			return nil
		},

		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "master",
			Name:      "event_queue_dispatches",
			Help:      "Number of dispatch events in the allocator mesos event queue.",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			count, ok := m["allocator/mesos/event_queue_dispatches"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/event_queue_dispatches not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(count)
			return nil
		},
		gauge("master", "allocator_offer_filters_active", "Number of active offer filters for all frameworks within the role", "role", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, err := regexp.Compile("allocator/mesos/offer_filters/roles/(.*?)/active")
			if err != nil {
				log.WithFields(log.Fields{
					"regex":  "allocator/mesos/offer_filters/roles/(.*?)/active",
					"Metric": "master_allocator_offer_filters_active",
					"error":  err,
				}).Error("could not compile regex")
				return fmt.Errorf("could not compile allocator_offer_filters_active regex: %s", err)
			}
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 2 {
					continue
				}
				role := matches[1]
				c.(*prometheus.GaugeVec).WithLabelValues(role, hostname).Set(value)
			}
			return nil
		},

		gauge("master", "allocator_role_quota_offered_or_allocated", "Amount of Resources considered offered or allocated towards a role's quota guarantee.", "role", "resource", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, err := regexp.Compile("allocator/mesos/quota/roles/(.*?)/Resources/(.*?)/offered_or_allocated")
			if err != nil {
				log.WithFields(log.Fields{
					"regex":  "allocator/mesos/quota/roles/(.*?)/Resources/(.*?)/offered_or_allocated",
					"Metric": "master_allocator_role_quota_offered_or_allocated",
					"error":  err,
				}).Error("could not compile regex")
				return fmt.Errorf("could not compile allocator_role_quota_offered_or_allocated regex: %s", err)
			}
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 3 {
					continue
				}
				role := matches[1]
				resource := matches[2]
				c.(*prometheus.GaugeVec).WithLabelValues(role, resource, hostname).Set(value)
			}
			return nil
		},

		gauge("master", "allocator_role_shares_dominant", "Dominance factor for a role", "role", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, err := regexp.Compile("allocator/mesos/roles/(.*?)/shares/dominant")
			if err != nil {
				log.WithFields(log.Fields{
					"regex":  "allocator/mesos/roles/(.*?)/shares/dominant",
					"Metric": "master_allocator_role_shares_dominant",
					"error":  err,
				}).Error("could not compile regex")
				return fmt.Errorf("could not compile allocator_role_shares_dominant regex: %s", err)
			}
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 2 {
					continue
				}
				role := matches[1]
				c.(*prometheus.GaugeVec).WithLabelValues(role, hostname).Set(value)
			}
			return nil
		},

		gauge("master", "allocator_role_quota_guarantee", "Amount of Resources guaranteed for a role via quota", "role", "resource", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, err := regexp.Compile("allocator/mesos/quota/roles/(.*?)/Resources/(.*?)/guarantee")
			if err != nil {
				log.WithFields(log.Fields{
					"regex":  "allocator/mesos/quota/roles/(.*?)/Resources/(.*?)/guarantee",
					"Metric": "master_allocator_role_quota_guarantee",
					"error":  err,
				}).Error("could not compile regex")
				return fmt.Errorf("could not compile allocator_role_quota_guarantee regex: %s", err)
			}
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 3 {
					continue
				}
				role := matches[1]
				resource := matches[2]
				c.(*prometheus.GaugeVec).WithLabelValues(role, resource, hostname).Set(value)
			}
			return nil
		},

		gauge("master", "allocator_resources_cpus", "Number of CPUs offered or allocated", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			total, ok := m["allocator/mesos/Resources/cpus/total"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/Resources/cpus/total not found in map")

			}
			offeredOrAllocated, ok := m["allocator/mesos/Resources/cpus/offered_or_allocated"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/Resources/cpus/offered_or_allocated not found in map")

			}

			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("offered_or_allocated", hostname).Set(offeredOrAllocated)
			return nil
		},

		gauge("master", "allocator_resources_disk", "Allocated or offered disk space in MB", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			total, ok := m["allocator/mesos/Resources/disk/total"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/Resources/disk/total not found in map")

			}
			offeredOrAllocated, ok := m["allocator/mesos/Resources/disk/offered_or_allocated"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/Resources/disk/offered_or_allocated not found in map")

			}

			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("offered_or_allocated", hostname).Set(offeredOrAllocated)
			return nil
		},

		gauge("master", "allocator_resources_mem", "Allocated or offered memory in MB", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			total, ok := m["allocator/mesos/Resources/mem/total"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/Resources/mem/total not found in map")

			}
			offeredOrAllocated, ok := m["allocator/mesos/Resources/mem/offered_or_allocated"]
			if !ok {

				return fmt.Errorf("key allocator/mesos/Resources/mem/offered_or_allocated not found in map")

			}

			c.(*prometheus.GaugeVec).WithLabelValues("total", hostname).Set(total)
			c.(*prometheus.GaugeVec).WithLabelValues("offered_or_allocated", hostname).Set(offeredOrAllocated)
			return nil
		},

		// Framework call counts (total)
		counter("master", "framework_calls_total", "Counts of API calls per Framework", "framework_name", "framework_id", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/calls`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 3 {
					continue
				}
				name := matches[1]
				id := matches[2]
				c.(*SettableCounterVec).Set(value, name, id, hostname)
			}
			return nil
		},

		// Framework call counts (by type)
		counter("master", "framework_calls", "Counts of API calls per Framework", "framework_name", "framework_id", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/calls/(.+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				typ := matches[3]
				c.(*SettableCounterVec).Set(value, name, id, typ, hostname)
			}
			return nil
		},

		// Framework offer operation counts (total)
		counter("master", "framework_operations_total", "Counts of offer operations per Framework", "framework_name", "framework_id", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/operations`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 3 {
					continue
				}
				name := matches[1]
				id := matches[2]
				c.(*SettableCounterVec).Set(value, name, id, hostname)
			}
			return nil
		},

		// Framework offer operation counts (by type)
		counter("master", "framework_operations", "Counts of offer operations per Framework", "framework_name", "framework_id", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/operations/(.+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				typ := matches[3]
				c.(*SettableCounterVec).Set(value, name, id, typ, hostname)
			}
			return nil
		},

		// Framework subscribed
		gauge("master", "framework_subscribed", "Boolean: is this Framework subscribed?", "framework_name", "framework_id", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/subscribed`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 2 {
					continue
				}
				name := matches[1]
				id := matches[2]
				c.(*prometheus.GaugeVec).WithLabelValues(name, id, hostname).Set(value)
			}
			return nil
		},

		// Framework role State
		gauge("master", "framework_suppressed", "Boolean: are offers for this role suppressed?", "framework_name", "framework_id", "role", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/roles/([^/]+)/suppressed`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				role := matches[3]
				c.(*prometheus.GaugeVec).WithLabelValues(name, id, role, hostname).Set(value)
			}
			return nil
		},

		// Framework events (total)
		counter("master", "framework_events_total", "Counts of events per Framework", "framework_name", "framework_id", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/events`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 3 {
					continue
				}
				name := matches[1]
				id := matches[2]
				c.(*SettableCounterVec).Set(value, name, id, hostname)
			}
			return nil
		},

		// Framework events (by type)
		counter("master", "framework_events", "Counts of events per Framework", "framework_name", "framework_id", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/events/([^/]+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				typ := matches[3]
				c.(*SettableCounterVec).Set(value, name, id, typ, hostname)
			}
			return nil
		},

		// Framework update events (by State)
		counter("master", "framework_update_events", "Counts of update events per Framework", "framework_name", "framework_id", "State", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/events/update/(.+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				state := matches[3]
				c.(*SettableCounterVec).Set(value, name, id, state, hostname)
			}
			return nil
		},

		// Framework Task active states
		gauge("master", "framework_task_states", "Task states per Framework", "framework_name", "framework_id", "State", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/tasks/([^/]+ing|task_lost)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 3 {
					continue
				}
				name := matches[1]
				id := matches[2]
				state := matches[3]

				c.(*prometheus.GaugeVec).WithLabelValues(name, id, state, hostname).Set(value)
			}
			return nil
		},

		// Framework terminal Task states
		counter("master", "framework_terminal_task_states", "Terminal Task states per Framework", "framework_name", "framework_id", "State", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			// TODO: don't use two regexes here
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/tasks/([^/]+)`)
			notre, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/tasks/([^/]+ing|task_lost)`)

			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				notmatches := notre.FindStringSubmatch(metric)
				if len(matches) != 4 || len(notmatches) == 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				state := matches[3]

				c.(*SettableCounterVec).Set(value, name, id, state, hostname)
			}
			return nil
		},

		// Framework Task failures by source reason
		counter("master", "framework_task_failures", "Framework Task failures by source reason", "framework_name", "framework_id", "source", "reason", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/tasks/task_failed/([^/]+)/([^/]+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 5 {
					continue
				}
				name := matches[1]
				id := matches[2]
				source := matches[3]
				reason := matches[4]

				c.(*SettableCounterVec).Set(value, name, id, source, reason, hostname)
			}
			return nil
		},

		// Framework offers
		counter("master", "framework_offers", "Number of offers by type per Framework", "framework_name", "framework_id", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/offers/(.+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				typ := matches[3]
				c.(*SettableCounterVec).Set(value, name, id, typ, hostname)
			}
			return nil
		},

		// Per-Framework resource allocation: offer filters
		counter("master", "framework_offer_filters", "Number of filters set per Framework over period", "framework_name", "framework_id", "period", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/allocation/offer_filters/refused_seconds/(.+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				pd := matches[3]
				c.(*SettableCounterVec).Set(value, name, id, pd, hostname)
			}
			return nil
		},

		// Per-Framework resource filters
		counter("master", "framework_resource_filters", "Number of times Resources were filtered per Framework", "framework_name", "framework_id", "reason", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/allocation/resources_filtered/(.+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 4 {
					continue
				}
				name := matches[1]
				id := matches[2]
				reason := matches[3]
				c.(*SettableCounterVec).Set(value, name, id, reason, hostname)
			}
			return nil
		},

		// Latest per-role DRF position
		gauge("master", "frameworks_drf_position", "Latest per-role DRF position", "framework_name", "framework_id", "role", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, _ := regexp.Compile(`master/frameworks/([^.]*)\.([^/]+)/allocation/roles/([^/]+)/latest_position/(.+)`)
			for metric, value := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 5 {
					continue
				}
				name := matches[1]
				id := matches[2]
				role := matches[3]
				typ := matches[4]
				c.(*prometheus.GaugeVec).WithLabelValues(name, id, role, typ, hostname).Set(value)
			}
			return nil
		},

		// Framework message Metrics
		counter("master", "frameworks_messages", "Messages passed around with the frameworks", "Framework", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			re, err := regexp.Compile("frameworks/(.*?)/messages_(.*?)$")
			if err != nil {
				log.WithFields(log.Fields{
					"regex":  "frameworks/(.*?)/messages_(.*?)$",
					"Metric": "master_frameworks_messages",
					"error":  err,
				}).Error("could not compile regex")
				return fmt.Errorf("could not compile frameworks_messages regex: %s", err)
			}
			for metric, messageCount := range m {
				matches := re.FindStringSubmatch(metric)
				if len(matches) != 3 {
					continue
				}
				framework := matches[1]
				messageStatus := matches[2]
				if len(framework) > 0 && len(messageStatus) > 0 {
					c.(*SettableCounterVec).Set(messageCount, framework, messageStatus, hostname)
				}
			}
			return nil
		},

		// Registrar stats
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "registrar",
			Name:      "registry_size_bytes",
			Help:      "Size of the registry in bytes",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			size, ok := m["registrar/registry_size_bytes"]
			if !ok {

				return fmt.Errorf("key registrar/registry_size_bytes not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(size)
			return nil
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "registrar",
			Name:      "queued_operations",
			Help:      "Number of operations in the registry queue",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			ops, ok := m["registrar/queued_operations"]
			if !ok {

				return fmt.Errorf("key registrar/queued_operations not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(ops)
			return nil
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "registrar",
			Name:      "state_fetch_ms",
			Help:      "Duration of State JSON fetch in ms",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			ms, ok := m["registrar/state_fetch_ms"]
			if !ok {

				return fmt.Errorf("key registrar/state_fetch_ms not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(ms)
			return nil
		},
		gauge("registrar", "state_store_ms", "Duration of State json store in ms.", "type", "hostname"): func(m MetricMap, c prometheus.Collector) error {
			mean, ok := m["registrar/state_store_ms"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms",
				}).Warn(LogErrNotFoundInMap)
			}
			min, ok := m["registrar/state_store_ms/min"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms/min",
				}).Warn(LogErrNotFoundInMap)
			}
			max, ok := m["registrar/state_store_ms/max"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms/max",
				}).Warn(LogErrNotFoundInMap)
			}
			p50, ok := m["registrar/state_store_ms/p50"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms/p50",
				}).Warn(LogErrNotFoundInMap)
			}
			p90, ok := m["registrar/state_store_ms/p90"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms/p90",
				}).Warn(LogErrNotFoundInMap)
			}
			p95, ok := m["registrar/state_store_ms/p95"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms/p95",
				}).Warn(LogErrNotFoundInMap)
			}
			p99, ok := m["registrar/state_store_ms/p99"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms/p99",
				}).Warn(LogErrNotFoundInMap)
			}
			p999, ok := m["registrar/state_store_ms/p999"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms/p999",
				}).Warn(LogErrNotFoundInMap)
			}
			p9999, ok := m["registrar/state_store_ms/p9999"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "registrar/state_store_ms/p9999",
				}).Warn(LogErrNotFoundInMap)
			}
			c.(*prometheus.GaugeVec).WithLabelValues("mean", hostname).Set(mean)
			c.(*prometheus.GaugeVec).WithLabelValues("min", hostname).Set(min)
			c.(*prometheus.GaugeVec).WithLabelValues("max", hostname).Set(max)
			c.(*prometheus.GaugeVec).WithLabelValues("p50", hostname).Set(p50)
			c.(*prometheus.GaugeVec).WithLabelValues("p90", hostname).Set(p90)
			c.(*prometheus.GaugeVec).WithLabelValues("p95", hostname).Set(p95)
			c.(*prometheus.GaugeVec).WithLabelValues("p99", hostname).Set(p99)
			c.(*prometheus.GaugeVec).WithLabelValues("p999", hostname).Set(p999)
			c.(*prometheus.GaugeVec).WithLabelValues("p9999", hostname).Set(p9999)
			return nil
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "registrar",
			Name:      "log_recovered",
			Help:      "Recovered Status of the registrar log",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			recovered, ok := m["registrar/log/recovered"]
			if !ok {

				return fmt.Errorf("key registrar/log/recovered not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(recovered)
			return nil
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "registrar",
			Name:      "log_ensemble_size",
			Help:      "Ensemble size of the registrar log",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			size, ok := m["registrar/log/ensemble_size"]
			if !ok {

				return fmt.Errorf("key registrar/log/ensemble_size not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(size)
			return nil
		},

		// Overlay log
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "overlay",
			Name:      "log_recovered",
			Help:      "Recovered Status of the overlay log",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			recovered, ok := m["overlay/log/recovered"]
			if !ok {

				return fmt.Errorf("key overlay/log/recovered not found in map")

			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(recovered)
			return nil
		},
		prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "mesos",
			Subsystem: "overlay",
			Name:      "log_ensemble_size",
			Help:      "Ensemble size of the overlay log",
		}, []string{"hostname"}): func(m MetricMap, c prometheus.Collector) error {
			size, ok := m["overlay/log/ensemble_size"]
			if !ok {
				log.WithFields(log.Fields{
					"name": "overlay/log_ensemble_size",
				}).Warn(LogErrNotFoundInMap)
			}
			c.(*prometheus.GaugeVec).WithLabelValues(hostname).Set(size)
			return nil
		},

		// END
	}
	return newMetricCollector(httpClient, metrics)
}
