param(
    [Parameter(ValueFromRemainingArguments=$true,Position=0)][string[]]$CargoArgs,
    [string]$ToolsRoot = $env:TIKTOK_LIVE_RUST_TOOLS_ROOT
)
$ErrorActionPreference = 'Stop'
$taskLocalTools = Join-Path $PSScriptRoot '.build-tools-root'
if (-not $ToolsRoot -and (Test-Path -LiteralPath $taskLocalTools)) {
    $ToolsRoot = (Get-Content -LiteralPath $taskLocalTools -Raw).Trim()
}
if (-not $ToolsRoot) { $ToolsRoot = Join-Path $PSScriptRoot '../..' }
$taskRoot = [System.IO.Path]::GetFullPath($ToolsRoot)
$taskRustup = Join-Path $taskRoot '.tools/rust/rustup'
$taskCargo = Join-Path $taskRoot '.tools/rust/cargo'
$taskToolchain = Join-Path $taskRustup 'toolchains/stable-x86_64-pc-windows-gnu'
$taskMingw = Join-Path $taskToolchain 'lib/rustlib/x86_64-pc-windows-gnu/bin/self-contained'
$taskCargoExe = Join-Path $taskCargo 'bin/cargo.exe'
$taskLinker = Join-Path $taskMingw 'x86_64-w64-mingw32-gcc.exe'
$taskZig = Join-Path $taskRoot '.tools/python/ziglang/zig.exe'
if ((Test-Path -LiteralPath $taskCargoExe) -and (Test-Path -LiteralPath $taskLinker) -and (Test-Path -LiteralPath $taskZig)) {
    $env:RUSTUP_HOME = $taskRustup
    $env:CARGO_HOME = $taskCargo
    $env:PATH = "$taskCargo\bin;$taskMingw;$env:PATH"
    $env:CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER = $taskLinker
    $env:TIKTOK_LIVE_RUST_ZIG = $taskZig
    $env:CARGO_ENCODED_RUSTFLAGS = '-C' + [char]31 + 'dlltool=' + (Join-Path $PSScriptRoot 'dlltool.cmd')
} else {
    $taskInstalledCargo = Get-Command cargo -CommandType Application -ErrorAction SilentlyContinue | Select-Object -First 1
    if (-not $taskInstalledCargo) { throw 'No Rust toolchain found. Install Rust with Cargo, or pass -ToolsRoot for a directory containing the isolated .tools toolchain.' }
    $taskCargoExe = $taskInstalledCargo.Source
}
Push-Location $PSScriptRoot
try {
    if (-not $CargoArgs) { $CargoArgs = @('test') }
    & $taskCargoExe @CargoArgs
    $taskExit = $LASTEXITCODE
} finally { Pop-Location }
exit $taskExit

