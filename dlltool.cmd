@echo off
if not defined TIKTOK_LIVE_RUST_ZIG exit /b 1
"%TIKTOK_LIVE_RUST_ZIG%" dlltool %*
