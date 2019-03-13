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

	unReadyTopDogJobs := getUnReadyTopDogJobs(ssn)
	returnReservedResources(ssn, unReadyTopDogJobs)

	for _, job := range ssn.Jobs {
		if ! eligibleForBackFill(ssn, job, unReadyTopDogJobs) {
			continue
		}

		glog.V(3).Infof("Back fill job %v", job)
		backFill(ssn, job)
	}
}

func (alloc *backfillAction) UnInitialize() {}

func returnReservedResources(ssn *framework.Session, jobs map[api.JobID]*api.JobInfo) {
	for _, job := range jobs {
		glog.V(3).Infof("Returning resources allocated to unready Top Dog Job <%v/%v>", job.Namespace, job.Name)

		for _, task := range job.Tasks {
			if task.Status == api.Allocated || task.Status == api.AllocatedOverBackfill {
				if err := job.UpdateTaskStatus(task, api.Pending); err != nil {
					glog.Errorf("Failed to update task <%v/%v> status to %v in Session <%v>: %v",
						task.Namespace, task.Name, api.Pending, ssn.UID, err)
				}

				node := ssn.NodeIndex[task.NodeName]
				node.RemoveTask(task)
				glog.V(4).Infof("Removed task %s from node %s. Idle: %+v; Used: %v; Releasing: %v.", task.Name, node.Name, node.Idle, node.Used, node.Releasing)
			}
		}
	}
}

// TODO: Move this function to plugin
func getUnReadyTopDogJobs(ssn *framework.Session) map[api.JobID]*api.JobInfo {
	unReadyTopDogJobs := make(map[api.JobID]*api.JobInfo)

	for _, job := range ssn.Jobs {
		if _, ok := ssn.TopDogReadyJobs[job.UID]; !ok {
			glog.V(3).Infof("Found unready Top Dog job <%v/%v>", job.Namespace, job.Name)
			unReadyTopDogJobs[job.UID] = job
		}
	}

	return unReadyTopDogJobs
}

func backFill(ssn *framework.Session, job *api.JobInfo) {
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
					glog.Errorf("Failed to bind Task %v on %v in Session %v", task.UID, node.Name, ssn.UID)
					continue
				}
			}
		}
	}

	if ! ssn.JobReady(job) {
		glog.V(3).Infof("Rolling back resources allocated to back fill job <%v/%v> because it is not ready to run", job.Namespace, job.Name)
		for _, task := range job.TaskStatusIndex[api.Allocated]	{
			node := ssn.NodeIndex[task.NodeName]
			node.RemoveTask(task)
			glog.V(3).Infof("Removed task <%v/%v> from node <%v>", task.Namespace, task.Name, node.Name)
		}
	}
}

// TODO: Move this function to plugin
func eligibleForBackFill(ssn *framework.Session, job *api.JobInfo, unReadyTopDogs map[api.JobID]*api.JobInfo) bool {
	// skip ready jobs
	if ssn.JobReady(job) {
		glog.Infof("ignored ready job %s for back fill", job.Name)
		return false
	}

	if _, found := unReadyTopDogs[job.UID]; found {
		glog.Infof("ignored non-ready top dog job %s for back fill", job.Name)
		return false
	}

	allPending := true
	for _, task := range job.Tasks {
		if task.Status != api.Pending {
			allPending = false
			break
		}
	}
	if !allPending {
		glog.Infof("ignored non-pending job %s for back fill", job.Name)
		return false
	}

	return true
}
