package cluster

import (
	"context"
	"encoding/base64"
	"os"
	"os/exec"
	"testing"
	"time"
	"unicode/utf16"

	"golang.org/x/sys/windows"
)

// This is the existing private supervisor role, hosted by the selected native
// PowerShell image. Its in-process P/Invoke adapter avoids the cross-architecture
// packaged-process creation boundary without adding another process owner.
// Keeping the native declarations and their single supervisor together makes
// handle, attribute-payload and suspended-process lifetimes directly auditable.
const diagNativeSupervisorSource = `
$ErrorActionPreference = 'Stop'
$private = $env:DXC_DIAG_WRAPPER_PRIVATE
$nonce = $env:DXC_DIAG_WRAPPER_NONCE
$image = $env:GOCLUSTER_TEST_POWERSHELL
$receiptSafe = $false
$result = [ordered]@{
    Nonce = $nonce; Status = 95; Context = ''; Error = ''; Image = $image
    Stage = 'identity'; NativeJoined = $false; NativeClosed = $true; HandleListExcluded = $false
}
try {
    if (-not [IO.Path]::IsPathFullyQualified($private) -or $nonce -cnotmatch '^[0-9a-f]{32}$') {
        throw 'invalid private diagnostic supervisor ownership'
    }
    $private = [IO.Path]::GetFullPath($private)
    if (-not [IO.Directory]::Exists($private) -or [IO.Path]::GetFileName($private) -cne $nonce -or
        ([IO.File]::GetAttributes($private) -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'invalid private diagnostic supervisor directory'
    }
    $receiptSafe = $true
    if (-not [IO.Path]::IsPathFullyQualified($image) -or
        -not [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFileName($image), 'pwsh.exe') -or
        -not [StringComparer]::OrdinalIgnoreCase.Equals([Environment]::ProcessPath, $image) -or
        -not [IO.File]::Exists($image)) {
        throw 'missing or mismatched actual PowerShell runner identity'
    }
    $script = $env:DXC_DIAG_WRAPPER_SCRIPT
    $root = [IO.Directory]::GetParent($private).Parent.FullName
    $expected = [IO.Path]::Combine($root, 'cmd', 'run-with-diag.ps1')
    if (-not [IO.Path]::IsPathFullyQualified($script) -or
        -not [StringComparer]::OrdinalIgnoreCase.Equals($script, $expected) -or
        -not [IO.File]::Exists($script) -or
        ([IO.File]::GetAttributes($script) -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'invalid copied diagnostic wrapper script'
    }
    $working = [IO.Path]::Combine($private, 'working directory')
    if (-not [IO.Directory]::Exists($working) -or
        ([IO.File]::GetAttributes($working) -band [IO.FileAttributes]::ReparsePoint)) {
        throw 'invalid private diagnostic working directory'
    }
    Add-Type -TypeDefinition @'
using System;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

public sealed class DiagNativeResult {
    public string Nonce { get; set; }
    public int Status { get; set; } = 95;
    public string Context { get; set; } = "";
    public string Error { get; set; } = "";
    public string Image { get; set; }
    public string Stage { get; set; } = "startup";
    public bool NativeJoined { get; set; }
    public bool NativeClosed { get; set; }
    public bool HandleListExcluded { get; set; }
}

public sealed class DiagNativeSupervisor {
    const uint Timeout = 258, Failed = 0xffffffff;
    const uint DuplicateSameAccess = 2, StartUseStdHandles = 0x100;
    const uint Suspended = 4, UnicodeEnvironment = 0x400, ExtendedStartup = 0x80000, NoWindow = 0x08000000;
    const int AttributeLimit = 4096, ReceiptLimit = 4096;
    IntPtr job, process, thread;
    readonly StartupResources startup = new StartupResources();
    bool joined, resumed, handleListExcluded;
    uint status;
    string stage = "startup";

    [StructLayout(LayoutKind.Sequential)]
    struct StartupInfo {
        public uint Size;
        public IntPtr Reserved, Desktop, Title;
        public uint X, Y, Width, Height, XChars, YChars, Fill, Flags;
        public ushort ShowWindow, ReservedBytes;
        public IntPtr ReservedData, Input, Output, Error;
    }
    [StructLayout(LayoutKind.Sequential)]
    struct StartupInfoEx { public StartupInfo Info; public IntPtr Attributes; }
    [StructLayout(LayoutKind.Sequential)]
    struct ProcessInfo { public IntPtr Process, Thread; public uint ProcessID, ThreadID; }
    [StructLayout(LayoutKind.Sequential)]
    struct SecurityAttributes {
        public uint Size;
        public IntPtr Descriptor;
        [MarshalAs(UnmanagedType.Bool)] public bool Inherit;
    }

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern IntPtr OpenJobObjectW(uint access, [MarshalAs(UnmanagedType.Bool)] bool inherit, string name);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool AssignProcessToJobObject(IntPtr job, IntPtr process);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool IsProcessInJob(IntPtr process, IntPtr job, [MarshalAs(UnmanagedType.Bool)] out bool member);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool QueryFullProcessImageNameW(IntPtr process, uint flags, StringBuilder image, ref uint size);
    [DllImport("kernel32.dll")]
    static extern IntPtr GetCurrentProcess();
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern IntPtr GetStdHandle(int kind);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    static extern IntPtr CreateFileW(string path, uint access, uint share, IntPtr security, uint disposition, uint flags, IntPtr template);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool CreatePipe(out IntPtr read, out IntPtr write, ref SecurityAttributes security, uint bytes);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool SetHandleInformation(IntPtr handle, uint mask, uint flags);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool PeekNamedPipe(IntPtr pipe, IntPtr buffer, uint bytes, IntPtr read, out uint available, IntPtr left);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool DuplicateHandle(IntPtr sourceProcess, IntPtr source, IntPtr targetProcess, out IntPtr target, uint access,
        [MarshalAs(UnmanagedType.Bool)] bool inherit, uint options);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool InitializeProcThreadAttributeList(IntPtr list, int count, uint flags, ref UIntPtr bytes);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool UpdateProcThreadAttribute(IntPtr list, uint flags, IntPtr attribute, IntPtr value, UIntPtr bytes,
        IntPtr previous, IntPtr previousBytes);
    [DllImport("kernel32.dll")]
    static extern void DeleteProcThreadAttributeList(IntPtr list);
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool CreateProcessW(string image, StringBuilder command, IntPtr processSecurity, IntPtr threadSecurity,
        [MarshalAs(UnmanagedType.Bool)] bool inherit, uint flags, IntPtr environment, string directory,
        ref StartupInfoEx startup, out ProcessInfo info);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern uint ResumeThread(IntPtr thread);
    [DllImport("kernel32.dll", SetLastError = true)]
    static extern uint WaitForSingleObject(IntPtr handle, uint milliseconds);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool GetExitCodeProcess(IntPtr process, out uint status);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool TerminateProcess(IntPtr process, uint status);
    [DllImport("kernel32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    static extern bool CloseHandle(IntPtr handle);

    static Exception NativeError(string operation) {
        int code = Marshal.GetLastWin32Error();
        return new Win32Exception(code, operation + ": Win32 0x" +
            unchecked((uint)code).ToString("X8", CultureInfo.InvariantCulture) + " (" + new Win32Exception(code).Message + ")");
    }
    static void Close(ref IntPtr handle) {
        if (handle == IntPtr.Zero) return;
        if (!CloseHandle(handle)) throw NativeError("close diagnostic native handle");
        handle = IntPtr.Zero;
    }
    static string ErrorText(Exception error) {
        string message = error.Message;
        return message.Length <= 768 ? message : message.Substring(0, 768);
    }
    static void RecordError(DiagNativeResult result, Exception error) {
        result.Error += (result.Error.Length == 0 ? "" : "; ") + ErrorText(error);
        if (result.Error.Length > 1536) result.Error = result.Error.Substring(0, 1536);
    }
    static void RequireMember(IntPtr process, IntPtr job) {
        bool member;
        if (!IsProcessInJob(process, job, out member)) throw NativeError("diagnostic job membership query");
        if (!member) throw new InvalidOperationException("diagnostic process is outside its intended job");
    }
    static uint Poll(IntPtr handle) {
        uint value = WaitForSingleObject(handle, 0);
        if (value == Failed) throw NativeError("diagnostic process liveness query");
        if (value != 0 && value != Timeout) throw new InvalidOperationException("unexpected diagnostic native wait " + value);
        return value;
    }
    static void RequireLiveMember(IntPtr process, IntPtr job) {
        if (Poll(process) != Timeout) throw new InvalidOperationException("diagnostic process is not live");
        RequireMember(process, job);
    }

    // Unmanaged attribute values stay allocated through CreateProcessW.
    // Only these three inheritable stdio duplicates enter HANDLE_LIST.
    sealed class StartupResources {
        public IntPtr Attributes;
        public readonly IntPtr[] Standard = new IntPtr[3];
        IntPtr input, jobValue, handleValue, policyValue, sentinelRead, sentinelWrite;
        bool initialized;
        public bool Closed {
            get {
                return input == IntPtr.Zero && Attributes == IntPtr.Zero &&
                    jobValue == IntPtr.Zero && handleValue == IntPtr.Zero && policyValue == IntPtr.Zero &&
                    Standard[0] == IntPtr.Zero && Standard[1] == IntPtr.Zero && Standard[2] == IntPtr.Zero &&
                    sentinelRead == IntPtr.Zero && sentinelWrite == IntPtr.Zero;
            }
        }
        public void Prepare(IntPtr job, string fault) {
            input = CreateFileW("NUL", 0x80000000, 3, IntPtr.Zero, 3, 0x80, IntPtr.Zero);
            if (input == new IntPtr(-1)) { input = IntPtr.Zero; throw NativeError("open diagnostic NUL stdin"); }
            IntPtr current = GetCurrentProcess();
            for (int i = 0; i < Standard.Length; i++) {
                if (fault == "stdio_acquisition_failure" && i == 2)
                    throw new InvalidOperationException("injected stdio acquisition failure after two owned duplicates");
                IntPtr source = i == 0 ? input : GetStdHandle(i == 1 ? -11 : -12);
                if (source == IntPtr.Zero || source == new IntPtr(-1))
                    throw new InvalidOperationException("missing actual diagnostic standard handle " + i);
                if (!DuplicateHandle(current, source, current, out Standard[i], 0, true, DuplicateSameAccess))
                    throw NativeError("duplicate actual diagnostic standard handle " + i);
            }
            bool sentinel = fault == "handle_list_exclusion" || fault == "handle_list_leak_control";
            if (sentinel) {
                var security = new SecurityAttributes { Size = (uint)Marshal.SizeOf(typeof(SecurityAttributes)), Inherit = true };
                if (!CreatePipe(out sentinelRead, out sentinelWrite, ref security, 0))
                    throw NativeError("create unrelated diagnostic inheritable pipe");
                if (!SetHandleInformation(sentinelRead, 1, 0))
                    throw NativeError("make unrelated diagnostic pipe reader non-inheritable");
            }
            UIntPtr bytes = UIntPtr.Zero;
            bool queried = InitializeProcThreadAttributeList(IntPtr.Zero, 3, 0, ref bytes);
            int queryError = Marshal.GetLastWin32Error();
            ulong size = bytes.ToUInt64();
            if (queried || queryError != 122 || size == 0 || size > AttributeLimit)
                throw new InvalidOperationException("invalid diagnostic native attribute allocation " + size + "/" + queryError);
            Attributes = Marshal.AllocHGlobal((int)size);
            if (!InitializeProcThreadAttributeList(Attributes, 3, 0, ref bytes))
                throw NativeError("initialize diagnostic native attributes");
            initialized = true;
            if (fault == "attribute_failure")
                throw new InvalidOperationException("injected attribute failure after initialized native allocation");
            jobValue = Marshal.AllocHGlobal(IntPtr.Size);
            // Keep supervisor admission intact; only creation-time JOB_LIST is invalid.
            Marshal.WriteIntPtr(jobValue, fault == "creation_job_refusal" ? IntPtr.Zero : job);
            int handleCount = fault == "handle_list_leak_control" ? 4 : 3;
            handleValue = Marshal.AllocHGlobal(handleCount * IntPtr.Size);
            for (int i = 0; i < Standard.Length; i++) Marshal.WriteIntPtr(handleValue, i * IntPtr.Size, Standard[i]);
            if (handleCount == 4) Marshal.WriteIntPtr(handleValue, 3 * IntPtr.Size, sentinelWrite);
            policyValue = Marshal.AllocHGlobal(4);
            Marshal.WriteInt32(policyValue, 2); // DESKTOP_APP_POLICY_DISABLE_PROCESS_TREE keeps this subtree in the job.
            Attribute(0x2000d, jobValue, IntPtr.Size);
            Attribute(0x20002, handleValue, handleCount * IntPtr.Size);
            Attribute(0x20012, policyValue, 4);
        }
        void Attribute(int key, IntPtr value, int bytes) {
            if (!UpdateProcThreadAttribute(Attributes, 0, new IntPtr(key), value, new UIntPtr((uint)bytes),
                IntPtr.Zero, IntPtr.Zero)) throw NativeError("update diagnostic native attribute " + key);
        }
        public bool VerifyHandleExclusion() {
            if (sentinelRead == IntPtr.Zero) return false;
            Close(ref sentinelWrite);
            uint available;
            if (PeekNamedPipe(sentinelRead, IntPtr.Zero, 0, IntPtr.Zero, out available, IntPtr.Zero))
                throw new InvalidOperationException("HANDLE_LIST leaked the unrelated inheritable pipe writer");
            if (Marshal.GetLastWin32Error() != 109) throw NativeError("verify unrelated diagnostic handle exclusion");
            return true;
        }
        static void Free(ref IntPtr allocation) {
            if (allocation == IntPtr.Zero) return;
            Marshal.FreeHGlobal(allocation);
            allocation = IntPtr.Zero;
        }
        public void Release() {
            string error = "";
            for (int i = 0; i < Standard.Length; i++) {
                try { Close(ref Standard[i]); } catch (Exception e) { error += ErrorText(e) + "; "; }
            }
            try { Close(ref input); } catch (Exception e) { error += ErrorText(e) + "; "; }
            try { Close(ref sentinelWrite); } catch (Exception e) { error += ErrorText(e) + "; "; }
            try { Close(ref sentinelRead); } catch (Exception e) { error += ErrorText(e) + "; "; }
            if (initialized) { DeleteProcThreadAttributeList(Attributes); initialized = false; }
            Free(ref Attributes);
            Free(ref jobValue);
            Free(ref handleValue);
            Free(ref policyValue);
            if (error.Length != 0) throw new InvalidOperationException(error);
        }
    }

    static void Publish(string path, byte[] bytes) {
        if (bytes.Length == 0 || bytes.Length > ReceiptLimit)
            throw new InvalidOperationException("diagnostic receipt size is outside its bound");
        string pending = path + ".pending";
        using (var file = new FileStream(pending, FileMode.CreateNew, FileAccess.Write, FileShare.None)) {
            file.Write(bytes, 0, bytes.Length);
        }
        File.Move(pending, path);
    }
    static byte[] ReadReceipt(string path) {
        using (var file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)) {
            if (file.Length == 0 || file.Length > ReceiptLimit)
                throw new InvalidOperationException("diagnostic replay receipt size is outside its bound");
            byte[] bytes = new byte[(int)file.Length];
            file.ReadExactly(bytes, 0, bytes.Length);
            if (file.ReadByte() != -1) throw new InvalidOperationException("diagnostic replay receipt grew");
            return bytes;
        }
    }
    static void PublishOwner(string directory, string name, string nonce, ulong handle, string stage) {
        string body = "{\"Nonce\":\"" + nonce + "\",\"Handle\":" + handle.ToString(CultureInfo.InvariantCulture) +
            ",\"Stage\":\"" + stage + "\"}";
        Publish(Path.Combine(directory, name), Encoding.UTF8.GetBytes(body));
    }
    static bool Marker(string directory, string name, string nonce) {
        try {
            using (var file = new FileStream(Path.Combine(directory, name), FileMode.Open, FileAccess.Read,
                FileShare.ReadWrite | FileShare.Delete)) {
                if (file.Length != nonce.Length) throw new InvalidOperationException("diagnostic " + name + " has invalid length");
                byte[] bytes = new byte[nonce.Length];
                file.ReadExactly(bytes, 0, bytes.Length);
                if (!String.Equals(Encoding.ASCII.GetString(bytes), nonce, StringComparison.Ordinal))
                    throw new InvalidOperationException("diagnostic " + name + " does not match its owner");
                return true;
            }
        } catch (IOException e) when ((e.HResult & 0xffff) == 2 || (e.HResult & 0xffff) == 3 || (e.HResult & 0xffff) == 32) {
            return false;
        }
    }
    static void AwaitMarker(string directory, string name, string nonce, Stopwatch clock, int milliseconds) {
        while (!Marker(directory, name, nonce)) {
            if (clock.ElapsedMilliseconds >= milliseconds)
                throw new TimeoutException("diagnostic " + name + ": context deadline exceeded");
            Thread.Sleep(5);
        }
    }
    static string Quote(string argument) {
        // Windows paths cannot contain a quote. Neither file argument ends in a
        // directory separator, so surrounding quotes preserve spaces verbatim.
        if (argument.IndexOf('"') >= 0 || argument.EndsWith("\\", StringComparison.Ordinal))
            throw new InvalidOperationException("invalid diagnostic native command argument");
        return "\"" + argument + "\"";
    }

    void Admit(string nonce, string fault) {
        job = OpenJobObjectW(5, false, "Local\\GoClusterDiagWrapper-" + nonce);
        if (job == IntPtr.Zero) throw NativeError("open parent diagnostic job");
        if (!AssignProcessToJobObject(fault == "job_refusal" ? IntPtr.Zero : job, GetCurrentProcess()))
            throw NativeError("assign native diagnostic supervisor to parent job");
        RequireLiveMember(GetCurrentProcess(), job);
    }
    void Create(string image, string script, string directory, string nonce, string fault, Stopwatch clock, int milliseconds) {
        try {
            startup.Prepare(job, fault);
            var info = new StartupInfoEx();
            info.Info.Size = (uint)Marshal.SizeOf(typeof(StartupInfoEx));
            info.Info.Flags = StartUseStdHandles;
            info.Info.Input = startup.Standard[0];
            info.Info.Output = startup.Standard[1];
            info.Info.Error = startup.Standard[2];
            info.Attributes = startup.Attributes;
            string selected = fault == "create_refusal" ? Path.Combine(directory, "missing-powershell.exe") : image;
            var command = new StringBuilder(Quote(image) + " -NoProfile -NonInteractive -File " + Quote(script));
            stage = "create";
            ProcessInfo created;
            // A null environment inherits this source's Unicode environment.
            // JOB_LIST, never post-create assignment, contains the suspended process.
            if (!CreateProcessW(selected, command, IntPtr.Zero, IntPtr.Zero, true,
                Suspended | ExtendedStartup | UnicodeEnvironment | NoWindow, IntPtr.Zero, directory, ref info, out created))
                throw NativeError("create atomically contained diagnostic PowerShell");
            process = created.Process;
            thread = created.Thread;
            stage = "created";
            if (fault == "owner_loss_after_create") {
                PublishOwner(Path.GetDirectoryName(directory), "created.json", nonce, unchecked((ulong)process.ToInt64()), stage);
                AwaitMarker(Path.GetDirectoryName(directory), "continue-created", nonce, clock, milliseconds);
            }
            handleListExcluded = startup.VerifyHandleExclusion();
        } finally { startup.Release(); }
    }
    void Verify(string image, string fault) {
        var actual = new StringBuilder(32768);
        uint size = (uint)actual.Capacity;
        if (!QueryFullProcessImageNameW(process, 0, actual, ref size))
            throw NativeError("query created diagnostic PowerShell image");
        if (!String.Equals(actual.ToString(), image, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("created diagnostic PowerShell image mismatch");
        if (fault == "membership_api_error") RequireMember(IntPtr.Zero, job);
        RequireLiveMember(process, job);
        if (fault == "suspended_verification_failure")
            throw new InvalidOperationException("injected suspended verification refusal after actual image/membership checks");
        stage = "verified";
    }
    void Transfer(string directory, string nonce, string fault, Stopwatch clock, int milliseconds) {
        ulong handle = unchecked((ulong)process.ToInt64());
        if (fault == "receipt_missing")
            throw new InvalidOperationException("injected missing PowerShell ownership receipt after verified creation");
        if (fault == "source_closed") { Close(ref process); stage = "source-closed"; }
        string path = Path.Combine(directory, "powershell.json");
        if (fault == "receipt_malformed") {
            Publish(path, Encoding.UTF8.GetBytes("{\"Nonce\":\"" + nonce + "\",\"Handle\":\"invalid\",\"Stage\":\"verified\"}"));
        } else if (fault == "receipt_replay") {
            Publish(path, ReadReceipt(Path.Combine(directory, "replay.json")));
        } else {
            string receiptNonce = fault == "receipt_wrong_nonce" ?
                (nonce == "00000000000000000000000000000000" ? "ffffffffffffffffffffffffffffffff" : "00000000000000000000000000000000") : nonce;
            PublishOwner(directory, "powershell.json", receiptNonce, handle, "verified");
        }
        // The creation-returned process handle remains open until the parent's
        // exact source-process DuplicateHandle and live-membership proof finish.
        AwaitMarker(directory, "adopted", nonce, clock, milliseconds);
        if (process == IntPtr.Zero) throw new InvalidOperationException("closed diagnostic ownership source cannot resume");
    }
    void Resume(string fault) {
        uint count = ResumeThread(fault == "resume_failure" ? IntPtr.Zero : thread);
        if (count == Failed) throw NativeError("resume diagnostic PowerShell");
        if (count != 1) throw new InvalidOperationException("unexpected diagnostic PowerShell suspend count " + count);
        resumed = true;
        stage = "resumed";
        Close(ref thread);
    }
    void Join(uint milliseconds) {
        if (process == IntPtr.Zero || joined) return;
        uint value = WaitForSingleObject(process, milliseconds);
        if (value == Failed) throw NativeError("join diagnostic PowerShell");
        if (value != 0) throw new InvalidOperationException("diagnostic PowerShell kernel wait incomplete: " + value);
        joined = true;
        if (!GetExitCodeProcess(process, out status)) throw NativeError("query diagnostic PowerShell terminal status");
    }
    void Stop() {
        if (process == IntPtr.Zero || joined) return;
        if (Poll(process) == Timeout && !TerminateProcess(process, 97)) {
            Exception error = NativeError("terminate owned diagnostic PowerShell");
            if (Poll(process) != 0) throw error;
        }
        Join(1500);
    }
    void Wait(string directory, string nonce, Stopwatch clock, int milliseconds) {
        while (Poll(process) == Timeout) {
            if (Marker(directory, "stop-powershell", nonce))
                throw new InvalidOperationException("injected PowerShell owner loss");
            if (clock.ElapsedMilliseconds >= milliseconds) throw new TimeoutException("context deadline exceeded");
            Thread.Sleep(5);
        }
        Join(0);
    }
    void Release(DiagNativeResult result) {
        try { Stop(); } catch (Exception e) { result.Status = 95; RecordError(result, e); }
        try { Close(ref thread); } catch (Exception e) { result.Status = 95; RecordError(result, e); }
        try { Close(ref process); } catch (Exception e) { result.Status = 95; RecordError(result, e); }
        try { startup.Release(); } catch (Exception e) { result.Status = 95; RecordError(result, e); }
        try { Close(ref job); } catch (Exception e) { result.Status = 95; RecordError(result, e); }
        result.Stage = stage;
        result.NativeJoined = joined;
        result.NativeClosed = process == IntPtr.Zero && thread == IntPtr.Zero && job == IntPtr.Zero && startup.Closed;
        result.HandleListExcluded = handleListExcluded;
    }
    public static DiagNativeResult Run(string image, string script, string directory, string nonce, string fault, int milliseconds) {
        var result = new DiagNativeResult { Nonce = nonce, Image = image };
        var owner = new DiagNativeSupervisor();
        var clock = Stopwatch.StartNew();
        try {
            owner.Admit(nonce, fault);
            Environment.SetEnvironmentVariable("DXC_DIAG_WRAPPER_CHILD", "1");
            owner.Create(image, script, Path.Combine(directory, "working directory"), nonce, fault, clock, milliseconds);
            owner.Verify(image, fault);
            owner.Transfer(directory, nonce, fault, clock, milliseconds);
            owner.Resume(fault);
            owner.Wait(directory, nonce, clock, milliseconds);
            result.Status = checked((int)owner.status);
        } catch (TimeoutException e) {
            result.Status = owner.resumed ? 94 : 95;
            if (owner.resumed) result.Context = "context deadline exceeded";
            RecordError(result, e);
        } catch (Exception e) {
            result.Status = 95;
            RecordError(result, e);
        } finally { owner.Release(result); }
        return result;
    }
}
'@
    $budget = 8000
    if ($env:DXC_DIAG_WRAPPER_STALL -eq '1') { $budget = 4000 }
    $result = [DiagNativeSupervisor]::Run($image, $script, $private, $nonce, $env:DXC_DIAG_WRAPPER_FAULT, $budget)
} catch {
    $result.Status = 95
    $result.Error = $_.Exception.Message
    if ($result.Error.Length -gt 1536) { $result.Error = $result.Error.Substring(0, 1536) }
}
try {
    if (-not $receiptSafe) { throw 'unsafe diagnostic result receipt directory' }
    $body = [Text.UTF8Encoding]::new($false).GetBytes(($result | ConvertTo-Json -Compress))
    if ($body.Length -gt 4096) { throw 'diagnostic supervisor receipt exceeds 4096 bytes' }
    $path = [IO.Path]::Combine($private, 'supervisor.json')
    $file = [IO.FileStream]::new($path + '.pending', [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
    try { $file.Write($body, 0, $body.Length) } finally { $file.Dispose() }
    [IO.File]::Move($path + '.pending', $path)
} catch {
    [Console]::Error.WriteLine('diagnostic supervisor result publication failed: ' + $_.Exception.Message)
    exit 95
}
if ($result.Error) { [Console]::Error.WriteLine($result.Error) }
exit ([int]$result.Status)
`

func TestDiagnosticWrapperNativeImageAccess(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Getenv(diagPowerShellIdentity), "-NoProfile", "-NonInteractive", "-Command", "exit 0")
	command.WaitDelay = 250 * time.Millisecond
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("plain selected-image launch before containment attributes: %v; %s", err, output)
	}
}

func TestDiagnosticWrapperNativeSupervisorAdmission(t *testing.T) {
	fixture := newDiagWrapperFixture(t, "native supervisor admission", 77, false, false)
	job, err := newDiagJob(fixture.nonce)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := windows.CloseHandle(job); err != nil {
			t.Error(err)
		}
	}()
	script := `
$ErrorActionPreference='Stop'
Add-Type -TypeDefinition @'
using System;using System.Runtime.InteropServices;using System.ComponentModel;
public static class AdmissionProbe {
 [DllImport("kernel32.dll",CharSet=CharSet.Unicode,SetLastError=true)]static extern IntPtr OpenJobObject(uint a,bool i,string n);
 [DllImport("kernel32.dll",SetLastError=true)]static extern bool AssignProcessToJobObject(IntPtr j,IntPtr p);
 [DllImport("kernel32.dll",SetLastError=true)]static extern bool IsProcessInJob(IntPtr p,IntPtr j,out bool m);
 [DllImport("kernel32.dll")]static extern IntPtr GetCurrentProcess();
 [DllImport("kernel32.dll",SetLastError=true)]static extern bool CloseHandle(IntPtr h);
 public static void Join(string n) {var j=OpenJobObject(5,false,n);if(j==IntPtr.Zero)throw new Win32Exception(Marshal.GetLastWin32Error(),"open");try{if(!AssignProcessToJobObject(j,GetCurrentProcess()))throw new Win32Exception(Marshal.GetLastWin32Error(),"assign");bool m;if(!IsProcessInJob(GetCurrentProcess(),j,out m)||!m)throw new Exception("not member");}finally{if(!CloseHandle(j))throw new Win32Exception(Marshal.GetLastWin32Error(),"close");}}
}
'@
[AdmissionProbe]::Join('Local\GoClusterDiagWrapper-` + fixture.nonce + `')
exit 0
`
	units := utf16.Encode([]rune(script))
	encoded := make([]byte, 2*len(units))
	for i, unit := range units {
		encoded[2*i], encoded[2*i+1] = byte(unit), byte(unit>>8)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Getenv(diagPowerShellIdentity), "-NoProfile", "-NonInteractive",
		"-EncodedCommand", base64.StdEncoding.EncodeToString(encoded))
	command.WaitDelay = 250 * time.Millisecond
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("actual native supervisor cannot join the parent-owned job: %v; %s", err, output)
	}
	accounting, err := diagAwaitJob(job)
	if err != nil || accounting.TotalProcesses < 1 || accounting.ActiveProcesses != 0 {
		t.Fatalf("native supervisor did not retire: %+v / %v", accounting, err)
	}
}
