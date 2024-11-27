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
#include "buffer_pool_manager.h"
#include "replacer/lru_replacer.h"
#include "replacer/lru_k_replacer.h"

#include "../../../common/error.h"

namespace wsdb {

BufferPoolManager::BufferPoolManager(DiskManager *disk_manager, LogManager *log_manager, size_t replacer_lru_k)
    : disk_manager_(disk_manager), log_manager_(log_manager)
{
  if (REPLACER == "LRUReplacer") {
    replacer_ = std::make_unique<LRUReplacer>();
  } else if (REPLACER == "LRUKReplacer") {
    replacer_ = std::make_unique<LRUKReplacer>(replacer_lru_k);
  } else {
    WSDB_FETAL("Unknown replacer: " + REPLACER);
  }
  // init free_list_
  for (frame_id_t i = 0; i < static_cast<int>(BUFFER_POOL_SIZE); i++) {
    free_list_.push_back(i);
  }
}

auto BufferPoolManager::FetchPage(file_id_t fid, page_id_t pid) -> Page *
{
  // WSDB_STUDENT_TODO(l1, t2);
  std::lock_guard<std::mutex> lock{latch_};

  fid_pid_t  fp{fid, pid};
  frame_id_t frame_id;
  if (page_frame_lookup_.contains(fp)) {
    frame_id = page_frame_lookup_[fp];
    replacer_->Pin(frame_id);
    frames_[frame_id].Pin();
  } else {
    frame_id = GetAvailableFrame();
    UpdateFrame(frame_id, fid, pid);
  }
  return frames_[frame_id].GetPage();
}

auto BufferPoolManager::UnpinPage(file_id_t fid, page_id_t pid, bool is_dirty) -> bool
{
  // WSDB_STUDENT_TODO(l1, t2);
  std::lock_guard<std::mutex> lock{latch_};

  fid_pid_t fp{fid, pid};
  if (!page_frame_lookup_.contains(fp)) {
    return false;
  }
  frame_id_t frame_id{page_frame_lookup_[fp]};
  Frame     &frame{frames_[frame_id]};
  if (!frame.InUse()) {
    return false;
  }

  frame.Unpin();
  if (!frame.InUse()) {
    replacer_->Unpin(frame_id);
  }
  if (!frame.IsDirty()) {
    frame.SetDirty(is_dirty);
  }
  return true;
}

auto BufferPoolManager::DeletePage(file_id_t fid, page_id_t pid) -> bool
{
  // WSDB_STUDENT_TODO(l1, t2);
  std::lock_guard<std::mutex> lock{latch_};

  fid_pid_t fp{fid, pid};
  if (!page_frame_lookup_.contains(fp)) {
    return false;
  }
  frame_id_t frame_id{page_frame_lookup_[fp]};
  Frame     &frame{frames_[frame_id]};
  if (frame.InUse()) {
    return false;
  }

  disk_manager_->WritePage(fid, pid, frame.GetPage()->GetData());
  frame.Reset();
  free_list_.push_front(frame_id);
  replacer_->Unpin(frame_id);
  // 此处不应有Unpin，在该frame为非inuse状态时replacer中必为unpin状态
  page_frame_lookup_.erase(fp);
  return true;
}

auto BufferPoolManager::DeleteAllPages(file_id_t fid) -> bool
{
  // WSDB_STUDENT_TODO(l1, t2);
  std::lock_guard<std::mutex> lock{latch_};

  bool delete_flag{true};
  for (auto it{page_frame_lookup_.begin()}; it != page_frame_lookup_.end();) {
    fid_pid_t fp{it->first};
    if (fp.fid == fid) {
      frame_id_t frame_id{page_frame_lookup_[fp]};
      Frame     &frame{frames_[frame_id]};
      if (frame.InUse()) {
        delete_flag &= false;
        continue;
      }

      disk_manager_->WritePage(fid, fp.pid, frame.GetPage()->GetData());
      frame.Reset();
      free_list_.push_front(frame_id);
      replacer_->Unpin(frame_id);
      // 此处不应有Unpin，在该frame为非inuse状态时replacer中必为unpin状态
      delete_flag &= true;
      it = page_frame_lookup_.erase(it);
    } else {
      ++it;
    }
  }
  return delete_flag;
}

auto BufferPoolManager::FlushPage(file_id_t fid, page_id_t pid) -> bool
{
  // WSDB_STUDENT_TODO(l1, t2);
  std::lock_guard<std::mutex> lock{latch_};

  fid_pid_t fp{fid, pid};
  if (!page_frame_lookup_.contains(fp)) {
    return false;
  }

  Frame &frame{frames_[page_frame_lookup_[fp]]};
  if (frame.IsDirty()) {
    disk_manager_->WritePage(fid, pid, frame.GetPage()->GetData());
  }
  return true;
}

auto BufferPoolManager::FlushAllPages(file_id_t fid) -> bool
{
  // WSDB_STUDENT_TODO(l1, t2);
  std::lock_guard<std::mutex> lock{latch_};

  for (auto &lookup_item : page_frame_lookup_) {
    if (lookup_item.first.fid == fid) {
      fid_pid_t fp{lookup_item.first};
      Frame    &frame{frames_[page_frame_lookup_[fp]]};
      if (frame.IsDirty()) {
        disk_manager_->WritePage(fid, fp.pid, frame.GetPage()->GetData());
      }
    }
  }
  return true;  // 没有情况返回false
}

auto BufferPoolManager::GetAvailableFrame() -> frame_id_t
{
  // WSDB_STUDENT_TODO(l1, t2);
  frame_id_t frame_id;
  if (free_list_.empty()) {
    if (!replacer_->Victim(&frame_id)) {
      WSDB_THROW(WSDB_NO_FREE_FRAME, "bufferpoolmanager缓存");
    }
  } else {
    frame_id = free_list_.front();
    free_list_.pop_front();
  }
  return frame_id;
}

void BufferPoolManager::UpdateFrame(frame_id_t frame_id, file_id_t fid, page_id_t pid)
{
  // WSDB_STUDENT_TODO(l1, t2);
  Frame    &frame{frames_[frame_id]};
  Page     &page{*frame.GetPage()};
  file_id_t ofid{page.GetFileId()};
  page_id_t opid{page.GetPageId()};
  if (frame.IsDirty()) {
    disk_manager_->WritePage(ofid, opid, page.GetData());
  }
  page_frame_lookup_.erase(fid_pid_t{ofid, opid});
  frame.Reset();
  frame.Pin();
  replacer_->Pin(frame_id);
  page.SetFilePageId(fid, pid);
  page_frame_lookup_.emplace(fid_pid_t{fid, pid}, frame_id);
  disk_manager_->ReadPage(fid, pid, page.GetData());
}

auto BufferPoolManager::GetFrame(file_id_t fid, page_id_t pid) -> Frame *
{
  const auto it = page_frame_lookup_.find({fid, pid});
  return it == page_frame_lookup_.end() ? nullptr : &frames_[it->second];
}

}  // namespace wsdb
