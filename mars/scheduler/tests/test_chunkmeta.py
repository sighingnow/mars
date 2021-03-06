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

import copy
import sys
import unittest
import uuid

from mars.actors import create_actor_pool, new_client
from mars.scheduler.chunkmeta import WorkerMeta, ChunkMetaStore, ChunkMetaCache, \
    ChunkMetaActor, ChunkMetaClient
from mars.scheduler.utils import SchedulerClusterInfoActor
from mars.tests.core import patch_method
from mars.utils import get_next_port


class Test(unittest.TestCase):
    def testChunkMetaStore(self):
        store = ChunkMetaStore()

        store['c0'] = WorkerMeta(0, (0,), ('w0',))
        self.assertIn('c0', store)
        self.assertEqual(store['c0'], WorkerMeta(0, (0,), ('w0',)))
        self.assertEqual(store.get('c0'), WorkerMeta(0, (0,), ('w0',)))
        self.assertIsNone(store.get('c1'))
        self.assertSetEqual(store.get_worker_chunk_keys('w0'), {'c0'})

        store['c0'] = WorkerMeta(0, (0,), ('w1',))
        self.assertEqual(store.get_worker_chunk_keys('w0'), set())
        self.assertSetEqual(store.get_worker_chunk_keys('w1'), {'c0'})

        del store['c0']
        self.assertNotIn('c0', store)

        store['c1'] = WorkerMeta(1, (1,), ('w0', 'w1'))
        store['c2'] = WorkerMeta(2, (2,), ('w1',))
        store['c3'] = WorkerMeta(3, (3,), ('w0',))
        store['c4'] = WorkerMeta(4, (4,), ('w0',))
        affected = store.remove_worker_keys('w0', lambda k: k[-1] < '4')
        self.assertListEqual(affected, ['c3'])
        self.assertEqual(store.get('c1'), WorkerMeta(1, (1,), ('w1',)))
        self.assertEqual(store.get('c2'), WorkerMeta(2, (2,), ('w1',)))
        self.assertSetEqual(store.get_worker_chunk_keys('w0'), {'c4'})
        self.assertSetEqual(store.get_worker_chunk_keys('w1'), {'c1', 'c2'})
        self.assertNotIn('c3', store)
        self.assertIn('c4', store)

        affected = store.remove_worker_keys('w0')
        self.assertListEqual(affected, ['c4'])
        self.assertNotIn('c4', store)
        self.assertIsNone(store.get_worker_chunk_keys('w0'))
        self.assertSetEqual(store.get_worker_chunk_keys('w1'), {'c1', 'c2'})

    def testChunkMetaCache(self):
        cache = ChunkMetaCache(9)

        for idx in range(10):
            cache['c%d' % idx] = WorkerMeta(idx, (idx,), ('w0',))
        self.assertNotIn('c0', cache)
        self.assertTrue(all('c%d' % idx in cache for idx in range(1, 10)))
        self.assertListEqual(sorted(cache.get_worker_chunk_keys('w0')),
                             ['c%d' % idx for idx in range(1, 10)])

        dup_cache = copy.deepcopy(cache)
        dup_cache.get('c1')
        dup_cache['c10'] = WorkerMeta(10, (10,), ('w0',))
        self.assertIsNone(dup_cache.get('c0'))
        self.assertNotIn('c2', dup_cache)
        self.assertIn('c1', dup_cache)
        self.assertTrue(all('c%d' % idx in dup_cache for idx in range(3, 11)))

        dup_cache = copy.deepcopy(cache)
        _ = dup_cache['c1']  # noqa: F841
        dup_cache['c10'] = WorkerMeta(10, (10,), ('w0',))
        self.assertNotIn('c2', dup_cache)
        self.assertIn('c1', dup_cache)
        self.assertTrue(all('c%d' % idx in dup_cache for idx in range(3, 11)))

        dup_cache = copy.deepcopy(cache)
        dup_cache['c1'] = WorkerMeta(1, (1,), ('w0',))
        dup_cache['c10'] = WorkerMeta(10, (10,), ('w0',))
        self.assertNotIn('c2', dup_cache)
        self.assertIn('c1', dup_cache)
        self.assertTrue(all('c%d' % idx in dup_cache for idx in range(3, 11)))

    @unittest.skipIf(sys.platform == 'win32', 'Currently not support multiple pools under Windows')
    @patch_method(ChunkMetaClient.get_scheduler)
    def testChunkMetaActors(self, *_):
        proc_count = 2
        endpoints = ['127.0.0.1:%d' % get_next_port() for _ in range(proc_count)]
        keys = []

        def _mock_get_scheduler(key):
            return endpoints[keys.index(key[1]) % len(endpoints)]

        ChunkMetaClient.get_scheduler.side_effect = _mock_get_scheduler

        session1 = str(uuid.uuid4())
        session2 = str(uuid.uuid4())
        with create_actor_pool(n_process=1, backend='gevent', address=endpoints[0]) as pool1:
            cluster_info1 = pool1.create_actor(SchedulerClusterInfoActor, endpoints,
                                               uid=SchedulerClusterInfoActor.default_uid())
            pool1.create_actor(ChunkMetaActor, uid=ChunkMetaActor.default_uid())

            with create_actor_pool(n_process=1, backend='gevent', address=endpoints[1]) as pool2:
                cluster_info2 = pool2.create_actor(SchedulerClusterInfoActor, endpoints,
                                                   uid=SchedulerClusterInfoActor.default_uid())
                pool2.create_actor(ChunkMetaActor, uid=ChunkMetaActor.default_uid())

                actor_client = new_client()
                client1 = ChunkMetaClient(actor_client, actor_client.actor_ref(cluster_info1))
                client2 = ChunkMetaClient(actor_client, actor_client.actor_ref(cluster_info2))

                loc_ref1 = actor_client.actor_ref(ChunkMetaActor.default_uid(), address=endpoints[0])
                loc_ref2 = actor_client.actor_ref(ChunkMetaActor.default_uid(), address=endpoints[1])

                key1 = (str(uuid.uuid4()), str(uuid.uuid4()))
                key2 = str(uuid.uuid4())
                key3 = str(uuid.uuid4())
                key4 = (str(uuid.uuid4()), str(uuid.uuid4()))
                key5 = str(uuid.uuid4())
                key6 = str(uuid.uuid4())
                keys = [key1, key2, key3, key4, key5, key6]
                client1.set_chunk_size(session1, key1, 512)
                client2.set_chunk_size(session1, key2, 1024)
                client2.set_chunk_size(session2, key3, 1024)

                self.assertEqual(client1.get_chunk_size(session1, key1), 512)
                self.assertEqual(client2.get_chunk_size(session1, key2), 1024)
                self.assertEqual(client1.get_chunk_size(session1, key2), 1024)
                self.assertEqual(client2.get_chunk_size(session1, key1), 512)

                self.assertListEqual(client1.batch_get_chunk_size(session1, [key1, key2]), [512, 1024])
                self.assertListEqual(client2.batch_get_chunk_size(session1, [key1, key2]), [512, 1024])

                client1.set_chunk_shape(session1, key1, (10,))
                client2.set_chunk_shape(session1, key2, (10,) * 2)
                client2.set_chunk_shape(session2, key3, (10,) * 2)

                self.assertEqual(client1.get_chunk_shape(session1, key1), (10,))
                self.assertEqual(client2.get_chunk_shape(session1, key2), (10,) * 2)
                self.assertEqual(client1.get_chunk_shape(session1, key2), (10,) * 2)
                self.assertEqual(client2.get_chunk_shape(session1, key1), (10,))

                self.assertListEqual(client1.batch_get_chunk_shape(session1, [key1, key2]), [(10,), (10,) * 2])
                self.assertListEqual(client2.batch_get_chunk_shape(session1, [key1, key2]), [(10,), (10,) * 2])

                mock_endpoint = '127.0.0.1:%d' % get_next_port()
                with create_actor_pool(n_process=1, backend='gevent', address=mock_endpoint) as pool3:
                    cluster_info3 = pool3.create_actor(SchedulerClusterInfoActor, endpoints,
                                                       uid=SchedulerClusterInfoActor.default_uid())
                    client3 = ChunkMetaClient(actor_client, actor_client.actor_ref(cluster_info3))
                    self.assertListEqual(client3.batch_get_chunk_shape(session1, [key1, key2]), [(10,), (10,) * 2])

                client1.add_worker(session1, key1, 'abc')
                client1.add_worker(session1, key1, 'def')
                client2.add_worker(session1, key2, 'ghi')

                client1.add_worker(session2, key3, 'ghi')

                self.assertEqual(sorted(client1.get_workers(session1, key1)), sorted(('abc', 'def')))
                self.assertEqual(sorted(client2.get_workers(session1, key2)), sorted(('ghi',)))

                batch_result = client1.batch_get_workers(session1, [key1, key2])
                self.assertEqual(sorted(batch_result[0]), sorted(('abc', 'def')))
                self.assertEqual(sorted(batch_result[1]), sorted(('ghi',)))

                affected = []
                for loc_ref in (loc_ref1, loc_ref2):
                    affected.extend(loc_ref.remove_workers_in_session(session2, ['ghi']))
                self.assertEqual(affected, [key3])
                self.assertEqual(sorted(client1.get_workers(session1, key2)), sorted(('ghi',)))
                self.assertIsNone(client1.get_workers(session2, key3))

                client1.delete_meta(session1, key1)
                self.assertIsNone(client1.get_workers(session1, key1))
                self.assertIsNone(client1.batch_get_chunk_size(session1, [key1, key2])[0])
                self.assertIsNone(client1.batch_get_workers(session1, [key1, key2])[0])

                client2.batch_delete_meta(session1, [key1, key2])
                self.assertIsNone(client1.get_workers(session1, key2))
                self.assertIsNone(client1.batch_get_chunk_size(session1, [key1, key2])[1])
                self.assertIsNone(client1.batch_get_workers(session1, [key1, key2])[1])

                meta4 = WorkerMeta(chunk_size=512, chunk_shape=(10,) * 2, workers=(endpoints[0],))
                loc_ref2.batch_set_chunk_meta(session1, [key4], [meta4])
                self.assertEqual(loc_ref2.get_chunk_meta(session1, key4).chunk_size, 512)
                self.assertEqual(loc_ref2.get_chunk_meta(session1, key4).chunk_shape, (10,) * 2)

                meta5 = WorkerMeta(chunk_size=512, chunk_shape=(10,) * 2, workers=(endpoints[0],))
                meta6 = WorkerMeta(chunk_size=512, chunk_shape=(10,) * 2, workers=(endpoints[0],))
                client1.batch_set_chunk_meta(session1, [key5, key6], [meta5, meta6])
                self.assertEqual(loc_ref1.get_chunk_meta(session1, key5).chunk_size, 512)
                self.assertEqual(loc_ref2.get_chunk_meta(session1, key6).chunk_size, 512)

    @unittest.skipIf(sys.platform == 'win32', 'Currently not support multiple pools under Windows')
    @patch_method(ChunkMetaClient.get_scheduler)
    def testChunkBroadcast(self, *_):
        proc_count = 2
        endpoints = ['127.0.0.1:%d' % get_next_port() for _ in range(proc_count)]
        keys = []

        def _mock_get_scheduler(key):
            return endpoints[keys.index(key[1]) % len(endpoints)]

        ChunkMetaClient.get_scheduler.side_effect = _mock_get_scheduler

        session_id = str(uuid.uuid4())
        with create_actor_pool(n_process=1, backend='gevent', address=endpoints[0]) as pool1:
            cluster_info1 = pool1.create_actor(SchedulerClusterInfoActor, endpoints,
                                               uid=SchedulerClusterInfoActor.default_uid())
            pool1.create_actor(ChunkMetaActor, uid=ChunkMetaActor.default_uid())

            with create_actor_pool(n_process=1, backend='gevent', address=endpoints[1]) as pool2:
                cluster_info2 = pool2.create_actor(SchedulerClusterInfoActor, endpoints,
                                                   uid=SchedulerClusterInfoActor.default_uid())
                pool2.create_actor(ChunkMetaActor, uid=ChunkMetaActor.default_uid())

                actor_client = new_client()
                client1 = ChunkMetaClient(actor_client, actor_client.actor_ref(cluster_info1))
                client2 = ChunkMetaClient(actor_client, actor_client.actor_ref(cluster_info2))
                local_ref1 = actor_client.actor_ref(ChunkMetaActor.default_uid(), address=endpoints[0])
                local_ref2 = actor_client.actor_ref(ChunkMetaActor.default_uid(), address=endpoints[1])

                key1 = str(uuid.uuid4())
                key2 = str(uuid.uuid4())
                key3 = str(uuid.uuid4())
                keys = [key1, key2, key3]

                client1.set_chunk_broadcasts(session_id, key1, [endpoints[1]])
                client1.set_chunk_size(session_id, key1, 512)
                client1.set_chunk_shape(session_id, key1, (10,) * 2)
                client1.add_worker(session_id, key1, 'abc')
                client2.set_chunk_broadcasts(session_id, key2, [endpoints[0]])
                client2.set_chunk_size(session_id, key2, 512)
                client1.set_chunk_shape(session_id, key2, (10,) * 2)
                client2.add_worker(session_id, key2, 'def')
                pool2.sleep(0.1)

                self.assertEqual(local_ref1.get_chunk_meta(session_id, key1).chunk_size, 512)
                self.assertEqual(local_ref1.get_chunk_meta(session_id, key1).chunk_shape, (10,) * 2)
                self.assertEqual(local_ref1.get_chunk_broadcasts(session_id, key1), [endpoints[1]])
                self.assertEqual(local_ref2.get_chunk_meta(session_id, key1).chunk_size, 512)
                self.assertEqual(local_ref2.get_chunk_meta(session_id, key1).chunk_shape, (10,) * 2)
                self.assertEqual(local_ref2.get_chunk_broadcasts(session_id, key2), [endpoints[0]])

                client1.batch_set_chunk_broadcasts(session_id, [key3], [[endpoints[1]]])
                meta3 = WorkerMeta(chunk_size=512, chunk_shape=(10,) * 2, workers=(endpoints[0],))
                local_ref1.batch_set_chunk_meta(session_id, [key3], [meta3])
                self.assertEqual(local_ref2.get_chunk_meta(session_id, key3).chunk_size, 512)
                self.assertEqual(local_ref2.get_chunk_meta(session_id, key3).chunk_shape, (10,) * 2)

                client1.delete_meta(session_id, key1)
                pool2.sleep(0.1)

                self.assertIsNone(local_ref1.get_chunk_meta(session_id, key1))
                self.assertIsNone(local_ref2.get_chunk_meta(session_id, key1))
                self.assertIsNone(local_ref1.get_chunk_broadcasts(session_id, key1))

                local_ref1.remove_workers_in_session(session_id, ['def'])
                local_ref2.remove_workers_in_session(session_id, ['def'])
                pool2.sleep(0.1)

                self.assertIsNone(local_ref1.get_chunk_meta(session_id, key2))
                self.assertIsNone(local_ref2.get_chunk_meta(session_id, key2))
                self.assertIsNone(local_ref2.get_chunk_broadcasts(session_id, key2))
