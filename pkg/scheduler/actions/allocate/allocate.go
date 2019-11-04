/*
Copyright 2017 The Kubernetes Authors.

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

package allocate

import (
	"fmt"

	"github.com/golang/glog"

	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/api"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/framework"
	"github.com/kubernetes-sigs/kube-batch/pkg/scheduler/util"
)

type allocateAction struct {
	ssn *framework.Session
}

func New() *allocateAction {
	return &allocateAction{}
}

func (alloc *allocateAction) Name() string {
	return "allocate"
}

func (alloc *allocateAction) Initialize() {}

func (alloc *allocateAction) Execute(ssn *framework.Session) {
	glog.V(3).Infof("Enter Allocate ...")
	defer glog.V(3).Infof("Leaving Allocate ...")

	// order queue based on QueueOrderFn, queuesPQ is a PQ containing "job queue"
	queuesPQ := util.NewPriorityQueue(ssn.QueueOrderFn)
	queueIdToPQMap := map[api.QueueID]*util.PriorityQueue{}

	// There are 3 PQs:
	// 1. A PQ of job queuesPQ
	// 2. Each job queue has a PQ of jobs
	// 3. Each job has a PQ of tasks

	// For each job
	for _, job := range ssn.Jobs {
		if queue, found := ssn.Queues[job.Queue]; found {
			// Push queue of the job to PQ
			queuesPQ.Push(queue)
		} else {
			glog.Warningf("Skip adding Job <%s/%s> because its queue %s is not found",
				job.Namespace, job.Name, job.Queue)
			continue
		}

		// push the job to the PQ of its corresponding job queue
		// TODO: the queueIdToPQMap can be abstracted by a member function of the queue
		if _, found := queueIdToPQMap[job.Queue]; !found {
			queueIdToPQMap[job.Queue] = util.NewPriorityQueue(ssn.JobOrderFn)
		}

		glog.V(4).Infof("Added Job <%s/%s> into Queue <%s>", job.Namespace, job.Name, job.Queue)
		queueIdToPQMap[job.Queue].Push(job)
	}

	glog.V(3).Infof("Try to allocate resource to %d Queues", len(queueIdToPQMap))

	allNodes := util.GetNodeList(ssn.Nodes)

	predicateFn := func(task *api.TaskInfo, node *api.NodeInfo) error {
		// Check for Resource Predicate
		// TODO: We could not allocate resource to task from both node.Idle and node.Releasing now,
		// after it is done, we could change the following compare to:
		// clonedNode := node.Idle.Clone()
		// if !task.InitResreq.LessEqual(clonedNode.Add(node.Releasing)) {
		//    ...
		// }
		if !task.InitResreq.LessEqual(node.Idle) && !task.InitResreq.LessEqual(node.Releasing) {
			return fmt.Errorf("task <%s/%s> ResourceFit failed on node <%s>",
				task.Namespace, task.Name, node.Name)
		}

		return ssn.RunPredicatePlugins(task, node)
	}

	for {
		if queuesPQ.Empty() {
			break
		}

		// Find the next most important queue
		queue := queuesPQ.Pop().(*api.QueueInfo)
		if ssn.RunOverusedPlugins(queue) {
			glog.V(3).Infof("Queue <%s> is overused, ignore it.", queue.Name)
			continue
		}

		jobsPQ, found := queueIdToPQMap[queue.UID]

		glog.V(3).Infof("Try to allocate resource to Jobs in Queue <%v>", queue.Name)

		if !found || jobsPQ.Empty() {
			glog.V(4).Infof("Can not find jobsPQ for queue %s.", queue.Name)
			continue
		}

		// Then find the most important job from the most important queue
		job := jobsPQ.Pop().(*api.JobInfo)
		tasksPQ := util.NewPriorityQueue(ssn.TaskOrderFn)
		for _, task := range job.TaskStatusIndex[api.Pending] {// find pending tasksPQ
			// Skip BestEffort task in 'allocate' action.
			if task.Resreq.IsEmpty() {
				glog.V(4).Infof("Task <%v/%v> is BestEffort task, skip it.",
					task.Namespace, task.Name)
				continue
			}

			tasksPQ.Push(task)
		}

		glog.V(3).Infof("Try to allocate resource to %d tasks of Job <%v/%v>",
			tasksPQ.Len(), job.Namespace, job.Name)

		for !tasksPQ.Empty() {
			// Then find next most important task of the job
			task := tasksPQ.Pop().(*api.TaskInfo)

			glog.V(3).Infof("There are <%d> nodes for Job <%v/%v>",
				len(ssn.Nodes), job.Namespace, job.Name)

			//any task that doesn't fit will be the last processed
			//within this loop context so any existing contents of
			//NodesFitDelta are for tasksPQ that eventually did fit on a
			//node
			if len(job.NodesFitDelta) > 0 {
				job.NodesFitDelta = make(api.NodeResourceMap)
			}

			// Run the predicate functions for the task and all nodes
			nodesLeft := util.FilterNodes(task, allNodes, predicateFn)
			if len(nodesLeft) == 0 {
				// Further Tasks should be checked because tasksPQ are ordered in priority, so it affects taskPriority within Job,
				// so if one task fails predicates, it should not check further tasksPQ in same job, should skip to next job.
				break
			}

			// Then run priority functions
			priorityList, err := util.PrioritizeNodes(task, nodesLeft, ssn.NodePrioritizers())
			if err != nil {
				glog.Errorf("Prioritize Nodes for task %s err: %v", task.UID, err)
				break
			}

			nodeName := util.SelectBestNode(priorityList)
			node := ssn.Nodes[nodeName]

			// Allocate idle resource to the task if node idle resource is enough for requested resource by the task
			if task.InitResreq.LessEqual(node.Idle) {
				glog.V(3).Infof("Binding Task <%v/%v> to node <%v>",
					task.Namespace, task.Name, node.Name)
				if err := ssn.Allocate(task, node.Name); err != nil {
					glog.Errorf("Failed to bind Task %v on %v in Session %v, err: %v",
						task.UID, node.Name, ssn.UID, err)
				}
			} else {
				//store information about missing resources
				job.NodesFitDelta[node.Name] = node.Idle.Clone()
				job.NodesFitDelta[node.Name].FitDelta(task.InitResreq)
				glog.V(3).Infof("Predicates failed for task <%s/%s> on node <%s> with limited resources",
					task.Namespace, task.Name, node.Name)

				// Allocate releasing resource to the task if any.
				if task.InitResreq.LessEqual(node.Releasing) {
					glog.V(3).Infof("Pipelining Task <%v/%v> to node <%v> for <%v> on <%v>",
						task.Namespace, task.Name, node.Name, task.InitResreq, node.Releasing)
					if err := ssn.Pipeline(task, node.Name); err != nil {
						glog.Errorf("Failed to pipeline Task %v on %v in Session %v",
							task.UID, node.Name, ssn.UID)
					}
				}
			}

			if ssn.JobReady(job) {
				jobsPQ.Push(job)
				break
			}
		}

		// Added Queue back until no job in Queue.
		queuesPQ.Push(queue)
	}
}

func (alloc *allocateAction) UnInitialize() {}
