# Copyright 1999-2018 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ..arithmetic.core import TensorUnaryOp
from ..array_utils import np, cp, sparse


class TensorSpecialOp(TensorUnaryOp):

    @classmethod
    def _get_func(cls, xp):
        if xp is np:
            from scipy import special
            return getattr(special, cls._func_name)
        elif cp is not None and xp is cp:
            from cupyx.scipy import special
            return getattr(special, cls._func_name)
        else:
            assert xp is sparse
            return getattr(sparse, cls._func_name)
