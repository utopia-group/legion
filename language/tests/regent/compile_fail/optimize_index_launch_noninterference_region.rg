-- Copyright 2021 Stanford University
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- runs-with:
-- [["-fflow", "0"]]

-- fails-with:
-- optimize_index_launch_noninterference_region.rg:33: loop optimization failed: argument 2 interferes with argument 1

import "regent"

task f(r : region(int), s : region(int)) where reads writes(r), reads (s) do
end

task main()
  var r = region(ispace(ptr, 5), int)
  var p = partition(equal, r, ispace(int1d, 3))
  fill(r, 0)

  __demand(__index_launch)
  for i = 0, 3 do
    f(p[i], r)
  end
end
regentlib.start(main)

