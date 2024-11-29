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
// Created by ziqi on 2024/7/18.
//

#include "executor_seqscan.h"

namespace wsdb {

SeqScanExecutor::SeqScanExecutor(TableHandle *tab) : AbstractExecutor(Basic), tab_(tab) {}

void SeqScanExecutor::Init()
{
  rid_ = tab_->GetFirstRID();

  // WSDB_STUDENT_TODO(l2, t1);
  if (rid_ == INVALID_RID) {
    is_end_ = true;
  }
}

void SeqScanExecutor::Next()
{
  // WSDB_STUDENT_TODO(l2, t1);
  WSDB_ASSERT(!is_end_, "SeqScanExecutor 已经结束");
  record_ = tab_->GetRecord(rid_);
  rid_    = tab_->GetNextRID(rid_);
  
  if (rid_ == INVALID_RID) {
    is_end_ = true;
  }
}

auto SeqScanExecutor::IsEnd() const -> bool
{
  // WSDB_STUDENT_TODO(l2, t1);
  return is_end_;
}

auto SeqScanExecutor::GetOutSchema() const -> const RecordSchema * { return &tab_->GetSchema(); }
}  // namespace wsdb