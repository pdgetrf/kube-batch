/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backfill

import (
	"github.com/golang/glog"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
)

type backfillAction struct {
	ssn *framework.Session
}

func New() *backfillAction {
	return &backfillAction{}
}

func (alloc *backfillAction) Name() string {
	return "backfill"
}

func (alloc *backfillAction) Initialize() {}

func (alloc *backfillAction) Execute(ssn *framework.Session) {
	glog.V(3).Infof("Enter Backfill ...")
	defer glog.V(3).Infof("Leaving Backfill ...")

	// TODO (k82cn): When backfill, it's also need to balance between Queues.
	for _, job := range ssn.Jobs {
		for _, task := range job.TaskStatusIndex[api.Pending] {
			if task.Resreq.IsEmpty() {
				// As task did not request resources, so it only need to meet predicates.
				// TODO (k82cn): need to prioritize nodes to avoid pod hole.
				for _, node := range ssn.Nodes {
					// TODO (k82cn): predicates did not consider pod number for now, there'll
					// be ping-pong case here.
					if err := ssn.PredicateFn(task, node); err != nil {
						glog.V(3).Infof("Predicates failed for task <%s/%s> on node <%s>: %v",
							task.Namespace, task.Name, node.Name, err)
						continue
					}

					glog.V(3).Infof("Binding Task <%v/%v> to node <%v>", task.Namespace, task.Name, node.Name)
					if err := ssn.Allocate(task, node.Name, false); err != nil {
						glog.Errorf("Failed to bind Task %v on %v in Session %v", task.UID, node.Name, ssn.UID)
						continue
					}
					break
				}
			} else {
			}
		}
	}

	// Collect back fill candidates
	backFillCandidates := make([]*api.JobInfo, 0, len(ssn.Jobs))
	for _, job := range ssn.Jobs {
		if ! eligibleForBackFill(job) {
			continue
		}
		backFillCandidates = append(backFillCandidates, job)
	}

	// Release resources allocated to unready top dog jobs so that
	// we can back fill more jobs in the next step.
	unReadyTopDogJobs := getUnReadyTopDogJobs(ssn)
	for _, job := range unReadyTopDogJobs {
		releaseReservedResources(ssn, job)
	}

	// Back fill
	for _, job := range backFillCandidates {
		backFill(ssn, job)
	}
}

func (alloc *backfillAction) UnInitialize() {}

// Releases resources allocated to the given job back to the cluster.
func releaseReservedResources(ssn *framework.Session, job *api.JobInfo) {
	glog.V(3).Infof("Releasing resources allocated to job <%v/%v>", job.Namespace, job.Name)

	for _, task := range job.Tasks {
		if task.Status == api.Allocated || task.Status == api.AllocatedOverBackfill {
			if err := job.UpdateTaskStatus(task, api.Pending); err != nil {
				glog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
					task.Namespace, task.Name, api.Pending, ssn.UID, err)
			}

			node := ssn.NodeIndex[task.NodeName]
			if err := node.RemoveTask(task); err != nil {
				glog.Errorf("Failed to remove task %v from node %v: %s", task.Name, node.Name, err)
				continue
			}

			glog.V(4).Infof("Removed task %s from node %s. Idle: %+v; Used: %v; Releasing: %v.", task.Name, node.Name, node.Idle, node.Used, node.Releasing)
		}
	}
}

// TODO Terry: Move this function to plugin
func getUnReadyTopDogJobs(ssn *framework.Session) map[api.JobID]*api.JobInfo {
	unReadyTopDogJobs := make(map[api.JobID]*api.JobInfo)

	for _, job := range ssn.Jobs {
		if ! ssn.JobReady(job) && ! isPendingJob(job) {
			glog.V(3).Infof("Found unready Top Dog job <%v/%v>", job.Namespace, job.Name)
			unReadyTopDogJobs[job.UID] = job
		}
	}

	return unReadyTopDogJobs
}

func backFill(ssn *framework.Session, job *api.JobInfo) {
	glog.V(3).Infof("Back fill job <%v/%v>", job.Namespace, job.Name)

	for _, task := range job.TaskStatusIndex[api.Pending] {
		task.IsBackfill = true
		for _, node := range ssn.Nodes {
			if err := ssn.PredicateFn(task, node); err != nil {
				glog.V(3).Infof("Predicates failed for task <%s/%s> on node <%s>: %v",
					task.Namespace, task.Name, node.Name, err)
				continue
			}

			if task.Resreq.LessEqual(node.Idle) {
				glog.V(3).Infof("Binding Task <%v/%v> to node <%v>", task.Namespace, task.Name, node.Name)
				if err := ssn.Allocate(task, node.Name, false); err != nil {
					glog.Errorf("Failed to bind Task %v on %v in Session %v: %s", task.UID, node.Name, ssn.UID, err)
					continue
				}
			}
		}
	}

	if ! ssn.JobReady(job) {
		releaseReservedResources(ssn, job)
	}
}

// TODO Terry: Move this function to plugin
func eligibleForBackFill(job *api.JobInfo) bool {
	return isPendingJob(job)
}

func isPendingJob(job *api.JobInfo) bool {
	allPending := true
	for _, task := range job.Tasks {
		if task.Status != api.Pending {
			allPending = false
			break
		}
	}

	return allPending
}