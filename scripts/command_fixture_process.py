"""Windows tool-tree owner for the private checkout runner.

The wrapper joins the parent's named Job Object before starting any tool. This
closes the launch/assignment race without undocumented APIs. Parent cancellation
first joins the wrapper, then terminates and joins all job members before the
stable executable lock can be released. Unsupported job assignment fails closed.
"""

import ctypes
from ctypes import wintypes
import os
import subprocess
import sys
import time
import uuid


class BasicLimits(ctypes.Structure):
    _fields_ = [("process_time", ctypes.c_int64), ("job_time", ctypes.c_int64),
                ("flags", wintypes.DWORD), ("min_working", ctypes.c_size_t),
                ("max_working", ctypes.c_size_t), ("active_limit", wintypes.DWORD),
                ("affinity", ctypes.c_size_t), ("priority", wintypes.DWORD),
                ("scheduling", wintypes.DWORD)]


class ExtendedLimits(ctypes.Structure):
    _fields_ = [("basic", BasicLimits), ("io_counters", ctypes.c_uint64 * 6),
                ("process_memory", ctypes.c_size_t), ("job_memory", ctypes.c_size_t),
                ("peak_process_memory", ctypes.c_size_t), ("peak_job_memory", ctypes.c_size_t)]


class Accounting(ctypes.Structure):
    _fields_ = [("times", ctypes.c_int64 * 4), ("page_faults", wintypes.DWORD),
                ("total", wintypes.DWORD), ("active", wintypes.DWORD), ("terminated", wintypes.DWORD)]


def kernel_api():
    kernel = ctypes.WinDLL("kernel32", use_last_error=True)
    signatures = {
        "CreateJobObjectW": ([ctypes.c_void_p, ctypes.c_wchar_p], wintypes.HANDLE),
        "OpenJobObjectW": ([wintypes.DWORD, wintypes.BOOL, ctypes.c_wchar_p], wintypes.HANDLE),
        "GetCurrentProcess": ([], wintypes.HANDLE),
        "AssignProcessToJobObject": ([wintypes.HANDLE, wintypes.HANDLE], wintypes.BOOL),
        "SetInformationJobObject": ([wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD], wintypes.BOOL),
        "QueryInformationJobObject": ([wintypes.HANDLE, ctypes.c_int, ctypes.c_void_p, wintypes.DWORD, ctypes.c_void_p], wintypes.BOOL),
        "TerminateJobObject": ([wintypes.HANDLE, wintypes.UINT], wintypes.BOOL),
        "CloseHandle": ([wintypes.HANDLE], wintypes.BOOL),
    }
    for name, (args, result) in signatures.items():
        function = getattr(kernel, name)
        function.argtypes, function.restype = args, result
    return kernel


def checked(value):
    if not value:
        raise ctypes.WinError(ctypes.get_last_error())
    return value


class ToolJob:
    def __init__(self):
        self.kernel = kernel_api()
        self.name = "Local\\GoClusterCommandTests-" + uuid.uuid4().hex
        self.handle = checked(self.kernel.CreateJobObjectW(None, self.name))
        try:
            limits = ExtendedLimits()
            limits.basic.flags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            checked(self.kernel.SetInformationJobObject(self.handle, 9, ctypes.byref(limits), ctypes.sizeof(limits)))
        except BaseException:
            self.close()
            raise

    def join(self, terminate=False):
        if terminate:
            checked(self.kernel.TerminateJobObject(self.handle, 1))
        deadline = time.monotonic() + 5
        while True:
            accounting = Accounting()
            checked(self.kernel.QueryInformationJobObject(self.handle, 1, ctypes.byref(accounting),
                                                          ctypes.sizeof(accounting), None))
            if accounting.active == 0:
                return
            if time.monotonic() >= deadline:
                raise TimeoutError("Windows tool job did not retire")
            time.sleep(.01)

    def close(self):
        checked(self.kernel.CloseHandle(self.handle))


def main():
    kernel = kernel_api()
    handle = checked(kernel.OpenJobObjectW(1, False, os.environ["GOCLUSTER_TEST_TOOL_JOB"]))
    try:
        # No tool is started unless assignment succeeds. Child tools inherit job
        # membership; no breakaway flag or nested independent owner is permitted.
        checked(kernel.AssignProcessToJobObject(handle, kernel.GetCurrentProcess()))
    finally:
        checked(kernel.CloseHandle(handle))
    return subprocess.call(sys.argv[1:])


if __name__ == "__main__":
    raise SystemExit(main())
