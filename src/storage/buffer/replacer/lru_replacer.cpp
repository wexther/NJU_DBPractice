/*------------------------------------------------------------------------------
 - Copyright (c) 2024. Websoft research group, Nanjing University.
 -
 - This program is free software: you can redistribute it and/or modify
 - it under the terms of the GNU General Public License as published by
 - the Free Software Foundation, either version 3 of the License, or
 - (at your option) any later version.
 -
 - This program is distributed in the hope that it will be useful,
 - but WITHOUT ANY WARRANTY; without even the implied warranty of
 - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 - GNU General Public License for more details.
 -
 - You should have received a copy of the GNU General Public License
 - along with this program.  If not, see <https://www.gnu.org/licenses/>.
 -----------------------------------------------------------------------------*/

//
// Created by ziqi on 2024/7/17.
//

#include "lru_replacer.h"
#include "common/config.h"
#include "../common/error.h"
namespace wsdb {

LRUReplacer::LRUReplacer() : cur_size_(0), max_size_(BUFFER_POOL_SIZE) {}

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool
{
  // WSDB_STUDENT_TODO(l1, t1);
  std::lock_guard<std::mutex> lock{latch_};

  for (auto list_it{lru_list_.rbegin()}; list_it != lru_list_.rend(); ++list_it) {
    if (list_it->second) {
      *frame_id = list_it->first;
      lru_hash_.erase(list_it->first);
      lru_list_.erase((++list_it).base());
      --cur_size_;
      return true;
    }
  }
  frame_id = nullptr;
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id)
{
  // WSDB_STUDENT_TODO(l1, t1);
  std::lock_guard<std::mutex> lock{latch_};

  auto frame_hash_it{lru_hash_.find(frame_id)};
  if (frame_hash_it == lru_hash_.end()) {
    // 页面不在缓存
    // 由FAQ，认为此时缓存未满，若需处理其余情况，解以下注释
    WSDB_ASSERT(lru_list_.size() < max_size_, "Pin不在缓存中的页面且缓存已满");
    // if (lru_list_.size() < max_size_) {
    //   // 缓存未满
    //   lru_list_.emplace_front(frame_id, false);
    //   lru_hash_[frame_id] = lru_list_.begin();
    // } else {
    //   // 缓存已满
    //   // 同victim的逻辑，因为有锁所以无法调用victim，或可将mutex改为recursive_mutex
    //   bool delete_flag{false};
    //   for (auto list_it {lru_list_.rbegin()}; list_it != lru_list_.rend(); ++list_it) {
    //     if (list_it->second) {
    //       lru_hash_.erase(list_it->first);
    //       lru_list_.erase((++list_it).base());
    //       delete_flag = true;
    //     }
    //   }
    //   if (!delete_flag) {
    //     WSDB_THROW(WSDB_NO_FREE_FRAME, "replacer中无可驱逐页面缓存");
    //   }
    lru_list_.emplace_front(frame_id, false);
    lru_hash_[frame_id] = lru_list_.begin();
    // }
  } else {
    // 页面在缓存
    if (frame_hash_it->second->second) {
      --cur_size_;
      frame_hash_it->second->second = false;
    }
    lru_list_.splice(lru_list_.begin(), lru_list_, frame_hash_it->second);
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id)
{
  // WSDB_STUDENT_TODO(l1, t1);
  std::lock_guard<std::mutex> lock{latch_};

  auto frame_hash_it{lru_hash_.find(frame_id)};
  // 由FAQ，认为此时页面已缓存，若需处理其余情况，解以下注释
  WSDB_ASSERT(frame_hash_it != lru_hash_.end(), "Unpin未缓存的页面");
  // if (frame_hash_it != lru_hash_.end()) {
  if (!frame_hash_it->second->second) {
    frame_hash_it->second->second = true;
    ++cur_size_;
  }  // ？不应有unpin两次或unpin未被pin过的页面的行为
  // }
}

auto LRUReplacer::Size() -> size_t
{
  // WSDB_STUDENT_TODO(l1, t1);
  std::lock_guard<std::mutex> lock{latch_};
  return cur_size_;
}//最好将该函数声明为const

}  // namespace wsdb
