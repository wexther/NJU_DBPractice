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
// Created by ziqi on 2024/8/10.
//

/**
 * @brief Limit the number of records returned by the child executor
 *
 */

#ifndef WSDB_EXECUTOR_LIMIT_H
#define WSDB_EXECUTOR_LIMIT_H

#include "executor_abstract.h"

namespace wsdb {
class LimitExecutor : public AbstractExecutor
{
public:
  LimitExecutor(AbstractExecutorUptr child, int limit);

  void Init() override;

  void Next() override;

  [[nodiscard]] auto IsEnd() const -> bool override;

  [[nodiscard]] auto GetOutSchema() const -> const RecordSchema * override;

private:
  const AbstractExecutorUptr child_;  // 更改声明为 const
  // max number of records to return，更改声明为 const
  const int limit_;
  // current number of records returned
  int count_;
  // 新加的
  bool is_end_;
};
}  // namespace wsdb

#endif  // WSDB_EXECUTOR_LIMIT_H
