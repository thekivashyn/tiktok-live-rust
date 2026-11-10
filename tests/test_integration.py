"""Real subprocess regressions; synthetic records only, no TikTok traffic."""
import asyncio
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location("consumer", Path(__file__).resolve().parents[1] / "examples/consume.py")
consumer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(consumer)


class ConsumerTests(unittest.IsolatedAsyncioTestCase):
    async def test_both_pipes_drain_and_large_additive_events_are_accepted(self):
        script = """
import json,sys
for _ in range(1000):
 print(json.dumps({'type':'status','data':{'state':'receiving','padding':'x'*128}}),file=sys.stderr)
print(json.dumps({'kind':'like','like_details':{'synthetic_padding':'x'*90000}}))
for _ in range(1000):
 print(json.dumps({'kind':'chat','data':{'text':'synthetic'}}))
print(json.dumps({'type':'report','data':{'stop_reason':'duration','error':None}}),file=sys.stderr)
"""
        result = await consumer.consume([sys.executable, "-c", script], timeout=10)
        self.assertEqual(result["events"], {"like": 1, "chat": 1000})
        self.assertEqual(result["last_state"], "receiving")
        self.assertEqual(result["stop_reason"], "duration")
        self.assertEqual(result["exit_code"], 0)
        self.assertNotIn("synthetic_padding", str(result))

    async def test_nonzero_exit_and_engine_error_are_preserved(self):
        script = "import sys; print('{\"type\":\"report\",\"data\":{\"stop_reason\":\"failed\",\"error\":{\"code\":\"no_events\"}}}',file=sys.stderr); sys.exit(7)"
        result = await consumer.consume([sys.executable, "-c", script], timeout=5)
        self.assertEqual(result["exit_code"], 7)
        self.assertEqual(result["error_code"], "no_events")

    async def assert_child_cleaned_up(self, script, timeout, error_type):
        children = []
        original = asyncio.create_subprocess_exec

        async def tracked(*args, **kwargs):
            child = await original(*args, **kwargs)
            children.append(child)
            return child

        with mock.patch.object(asyncio, "create_subprocess_exec", side_effect=tracked):
            with self.assertRaises(error_type):
                await consumer.consume([sys.executable, "-u", "-c", script], timeout=timeout)
        self.assertEqual(len(children), 1)
        self.assertIsNotNone(children[0].returncode)

    async def test_invalid_output_terminates_the_owned_child(self):
        await self.assert_child_cleaned_up(
            "import time; print('invalid-json',flush=True); time.sleep(60)", 5, ExceptionGroup)

    async def test_host_deadline_terminates_the_owned_child(self):
        await self.assert_child_cleaned_up("import time; time.sleep(60)", 0.2, TimeoutError)


if __name__ == "__main__":
    unittest.main()
