package main

import (
	"fmt"
	"slices"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v3"
	"github.com/sflanaga/statticker"
)

const NULL_USER_ID = ^uint32(0)

var switchUserCount uint64 = 0

type UserStats struct {
	uid       uint32
	size      uint64
	filecount uint64
	dircount  uint64
}

func (user *UserStats) clear(uid uint32) {
	user.uid = uid
	user.dircount = 0
	user.filecount = 0
	user.size = 0
}

func switchUser(user *UserStats, uid uint32) {
	if user.uid != NULL_USER_ID {
		atomic.AddUint64(&switchUserCount, 1)
	}
	loadUserInfo(*user)
	user.clear(uid)
	// println("switch user: ", user.uid)
}

func (user *UserStats) addDir(uid uint32) {
	if user.uid == NULL_USER_ID {
		user.uid = uid
	}
	if uid == user.uid {
		user.dircount += 1
	} else {
		switchUser(user, uid)
	}
}

func (user *UserStats) addFile(uid uint32, size uint64) {
	if user.uid == NULL_USER_ID {
		user.uid = uid
	}
	if uid == user.uid {
		user.filecount += 1
		user.size += size
		// fmt.Printf("acc user: %v\n", *user)
	} else {
		switchUser(user, uid)
	}
}

var userMap = xsync.NewMapOf[uint32, UserStats]()

func loadUserInfo(userInfo UserStats) {
	if userInfo.uid == NULL_USER_ID {
		// println("skipping user load>>>> ", userInfo.uid)
		return
	} else {
		// fmt.Printf("loading user load>>>>, %v\n", userInfo)

		userMap.Compute(userInfo.uid, func(oldValue UserStats, loaded bool) (newValue UserStats, delete bool) {
			if !loaded {
				// println("adding")
				return userInfo, false
			} else {
				// println("updating")
				oldValue.dircount += userInfo.dircount
				oldValue.filecount += userInfo.filecount
				oldValue.size += userInfo.size
				return oldValue, false

			}
		})
	}

}

func diffu64(a, b uint64) int64 {
	var d int64
	if a&0x8000_0000_0000_0000 > 0 || b&0x8000_0000_0000_0000 > 0 {
		// so we do 63 bit only diffs... so compromise a bit
		a = a >> 1
		b = b >> 1
	}
	d = int64(a) - int64(b)
	return d
}

func cmpUserStatsSort(i, j UserStats) int {
	size_d := diffu64(i.size, j.size)
	if size_d == 0 {
		node_d := diffu64(i.filecount+i.dircount, j.filecount+j.dircount)
		return -int(node_d)
	} else {
		return -int(size_d)
	}
}

func printUserInfo(limit int) {
	if userMap.Size() > 0 {
		fmt.Printf("%6s  %8s %8s %8s   uniq users: %d, switch users: %d\n", "UID", "Space", "Files", "Dirs", userMap.Size(), switchUserCount)
		var list = make([]UserStats, userMap.Size())
		userMap.Range(func(key uint32, value UserStats) bool {
			list = append(list, value)
			return true
		})
		slices.SortFunc(list, cmpUserStatsSort)
		for i, value := range list {
			fmt.Printf("%6d  %8s %8s %8s \n",
				value.uid,
				statticker.FormatBytes(value.size),
				statticker.AddCommas(value.filecount),
				statticker.AddCommas(value.dircount))
			if i >= limit {
				break
			}
		}
		fmt.Println()
	}
}
