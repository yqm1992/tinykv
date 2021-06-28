// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"go.uber.org/zap"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).

	storeArray := cluster.GetStores()
	suitableStores := make([]*core.StoreInfo, 0)
	maxStoreDownTime := cluster.GetMaxStoreDownTime()
	for _, curStore := range storeArray {
		if curStore.IsUp() && curStore.DownTime() <= maxStoreDownTime {
			suitableStores = append(suitableStores, curStore)
		}
	}
	if len(suitableStores) < 2 {
		return nil
	}
	// sort by desc
	sort.Slice(suitableStores, func(i, j int) bool {
		return suitableStores[i].GetRegionSize() > suitableStores[j].GetRegionSize()
	})
	targetStore := suitableStores[len(suitableStores)-1]
	getRegionFuncs := make([]func(storeID uint64, opts ...core.RegionOption) *core.RegionInfo, 3)
	getRegionFuncs[0] = cluster.RandPendingRegion
	getRegionFuncs[1] = cluster.RandFollowerRegion
	getRegionFuncs[2] = cluster.RandLeaderRegion
	for _, curStore := range suitableStores {
		// src and dst store can not be the same one
		if curStore == targetStore {
			break
		}
		srcStore := curStore
		srcID := srcStore.GetID()
		for j := 0; j < len(getRegionFuncs); j++ {
			getRegionFunc := getRegionFuncs[j]
			for  k := 0; k < balanceRegionRetryLimit; k++ {
				srcRegion := getRegionFunc(srcID)
				if srcRegion != nil {
					if op := s.createOperator(cluster, srcRegion, srcStore, targetStore); op != nil {
						return op
					}
				}
			}
		}
		log.Debug("no operator created for selected stores", zap.String("scheduler", s.GetName()), zap.Uint64("source", srcID))
	}
	return nil
}

// createOperator creates the operator according to the source and target store.
// If the difference between the two stores is tolerable, then
// no new operator need to be created, otherwise create an operator that moves
// region from the source store to the target store
func (s *balanceRegionScheduler) createOperator(cluster opt.Cluster, region *core.RegionInfo, source, target *core.StoreInfo) *operator.Operator {
	if source.GetRegionSize() - target.GetRegionSize() < 2*region.GetApproximateSize() {
		return nil
	}
	newPeer, err := cluster.AllocPeer(target.GetID())
	if err != nil {
		return nil
	}
	sourceID := source.GetID()
	targetID := target.GetID()
	newPeerId := newPeer.GetId()
	if op, err := operator.CreateMovePeerOperator("balance-region", cluster, region, operator.OpBalance, sourceID, targetID, newPeerId); err == nil {
		return op
	}
	return nil
}