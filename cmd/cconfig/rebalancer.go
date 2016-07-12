// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package main

import (
	"strconv"
	"time"

	"github.com/wandoulabs/zkhelper"

	"github.com/CodisLabs/codis/pkg/models"
	"github.com/CodisLabs/codis/pkg/utils"
	"github.com/CodisLabs/codis/pkg/utils/errors"
	"github.com/CodisLabs/codis/pkg/utils/log"
)

type NodeInfo struct {
	GroupId   int
	CurSlots  []int
	MaxMemory int64
}

// 获取各组->SLOT映射关系
func getLivingNodeInfos(zkConn zkhelper.Conn) ([]*NodeInfo, error) {
	// 获取所有分组列表
	groups, err := models.ServerGroups(zkConn, globalEnv.ProductName())
	if err != nil {
		return nil, errors.Trace(err)
	}

	// 获取所有SLOT列表
	slots, err := models.Slots(zkConn, globalEnv.ProductName())

	// 构建gid->slot.Id映射表
	slotMap := make(map[int][]int)
	for _, slot := range slots {
		if slot.State.Status == models.SLOT_STATUS_ONLINE {
			slotMap[slot.GroupId] = append(slotMap[slot.GroupId], slot.Id)
		}
	}
	var ret []*NodeInfo
	for _, g := range groups {
		master, err := g.Master(zkConn) // 获取分组MASTER信息
		if err != nil {
			return nil, errors.Trace(err)
		}
		if master == nil {
			return nil, errors.Errorf("group %d has no master", g.Id)
		}
		out, err := utils.GetRedisConfig(master.Addr, globalEnv.Password(), "maxmemory")
		if err != nil {
			return nil, errors.Trace(err)
		}
		maxMem, err := strconv.ParseInt(out, 10, 64)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if maxMem <= 0 {
			return nil, errors.Errorf("redis %s should set maxmemory", master.Addr)
		}
		node := &NodeInfo{
			GroupId:   g.Id,          // 组ID
			CurSlots:  slotMap[g.Id], // gid -> slot.id列表
			MaxMemory: maxMem,        // 最大内存
		}
		ret = append(ret, node)
	}
	cnt := 0
	for _, info := range ret {
		cnt += len(info.CurSlots)
	}
	if cnt != models.DEFAULT_SLOT_NUM {
		return nil, errors.Errorf("not all slots are online")
	}
	return ret, nil
}

// 获取各组->SLOT配额个数映射
func getQuotaMap(zkConn zkhelper.Conn) (map[int]int, error) {
	nodes, err := getLivingNodeInfos(zkConn)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ret := make(map[int]int)
	var totalMem int64
	totalQuota := 0
	for _, node := range nodes {
		totalMem += node.MaxMemory // 统计总内存
	}

	// 根据各组内存的大小计算各组SLOT配额
	for _, node := range nodes {
		quota := int(models.DEFAULT_SLOT_NUM * node.MaxMemory * 1.0 / totalMem)
		ret[node.GroupId] = quota
		totalQuota += quota // 总配额
	}

	// round up
	if totalQuota < models.DEFAULT_SLOT_NUM {
		for k, _ := range ret {
			ret[k] += models.DEFAULT_SLOT_NUM - totalQuota // 剩余配额放在第一个组
			break
		}
	}

	return ret, nil
}

// experimental simple auto rebalance :)
func Rebalance() error {
	// 获取各组->内存配额映射表(gid -> quota)
	targetQuota, err := getQuotaMap(safeZkConn)
	if err != nil {
		return errors.Trace(err)
	}

	// 获取各组->SLOT映射关系(gid -> slot.id list)
	livingNodes, err := getLivingNodeInfos(safeZkConn)
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("start rebalance")
	// 遍历各组->SLOT映射
	for _, node := range livingNodes {
		// 如果当前node组中SLOT个数过度(大于配额个数), 则将该组中的SLOT分给其他组
		// 优点: 做到尽量少的数据做迁移
		for len(node.CurSlots) > targetQuota[node.GroupId] {
			// 遍历各组->SLOT映射
			for _, dest := range livingNodes {
				// 查找SLOT数少于配额的分组, 并将node组中1个SLOT迁移给改组
				if dest.GroupId != node.GroupId &&
					len(dest.CurSlots) < targetQuota[dest.GroupId] &&
					len(node.CurSlots) > targetQuota[node.GroupId] {
					// 将node组最后1个SLOT迁移给该组
					slot := node.CurSlots[len(node.CurSlots)-1]
					// create a migration task
					info := &MigrateTaskInfo{
						Delay:      0,
						SlotId:     slot,
						NewGroupId: dest.GroupId,
						Status:     MIGRATE_TASK_PENDING,
						CreateAt:   strconv.FormatInt(time.Now().Unix(), 10),
					}
					globalMigrateManager.PostTask(info)

					node.CurSlots = node.CurSlots[0 : len(node.CurSlots)-1]
					dest.CurSlots = append(dest.CurSlots, slot)
				}
			}
		}
	}
	log.Infof("rebalance tasks submit finish")
	return nil
}
