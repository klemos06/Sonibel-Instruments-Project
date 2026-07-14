from __future__ import annotations

import os
import shlex
import subprocess
import time
from pathlib import Path


GPIO_PIN = int(os.environ.get("GPIO_PIN", "6"))
GPIO_PULL_UP = os.environ.get("GPIO_PULL_UP", "true").strip().lower() in {"1", "true", "yes", "on"}
POWER_LOSS_ACTIVE_STATE = int(os.environ.get("POWER_LOSS_ACTIVE_STATE", "0"))
POLL_SECONDS = float(os.environ.get("POLL_SECONDS", "0.10"))
DEBOUNCE_SECONDS = float(os.environ.get("DEBOUNCE_SECONDS", "1.0"))
COOLDOWN_SECONDS = float(os.environ.get("COOLDOWN_SECONDS", "10.0"))
UPLOAD_SERVICE = os.environ.get("UPLOAD_SERVICE", "unmodified-daq-power-loss-flow.service")
DAQ_SERVICES = os.environ.get("DAQ_SERVICES", "")
TEST_TRIGGER_FILE = Path(os.environ.get("TEST_TRIGGER_FILE", "/tmp/force-power-loss-upload"))


def log(message: str) -> None:
    print(f"{time.strftime('%Y-%m-%dT%H:%M:%S%z')} {message}", flush=True)


def split_services(value: str) -> list[str]:
    services: list[str] = []
    for chunk in value.replace(",", " ").split():
        item = chunk.strip()
        if item:
            services.append(item)
    return services


def run_systemctl(args: list[str], check: bool = False) -> subprocess.CompletedProcess[str]:
    cmd = ["systemctl", *args]
    log("Running: " + " ".join(shlex.quote(part) for part in cmd))
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.stdout:
        log(result.stdout.strip())
    if result.stderr:
        log(result.stderr.strip())
    if check and result.returncode != 0:
        raise RuntimeError(f"Command failed with {result.returncode}: {' '.join(cmd)}")
    return result


def stop_daq_services() -> None:
    services = split_services(DAQ_SERVICES)
    if not services:
        log("No DAQ services configured to stop.")
        return
    for service in services:
        run_systemctl(["stop", service], check=False)


def trigger_upload(reason: str) -> None:
    log(f"Power-loss trigger detected: {reason}")
    stop_daq_services()
    run_systemctl(["start", UPLOAD_SERVICE], check=False)


def main() -> int:
    try:
        from gpiozero import DigitalInputDevice
    except Exception as exc:
        log(f"ERROR: gpiozero import failed: {type(exc).__name__}: {exc}")
        log("Install it with: sudo apt install -y python3-gpiozero")
        return 2

    device = DigitalInputDevice(GPIO_PIN, pull_up=GPIO_PULL_UP)
    active_state = 1 if POWER_LOSS_ACTIVE_STATE else 0
    log(
        f"Watching GPIO{GPIO_PIN}; pull_up={GPIO_PULL_UP}; "
        f"power_loss_active_state={active_state}; upload_service={UPLOAD_SERVICE}"
    )
    log(f"Software test trigger file: {TEST_TRIGGER_FILE}")

    active_since: float | None = None
    armed = True
    last_trigger = 0.0

    while True:
        now = time.monotonic()

        if TEST_TRIGGER_FILE.exists():
            try:
                TEST_TRIGGER_FILE.unlink()
            except OSError:
                pass
            if now - last_trigger >= COOLDOWN_SECONDS:
                trigger_upload(f"software trigger file {TEST_TRIGGER_FILE}")
                last_trigger = now
                armed = False
                active_since = None
            time.sleep(POLL_SECONDS)
            continue

        current_state = 1 if device.value else 0
        is_active = current_state == active_state

        if is_active and armed:
            if active_since is None:
                active_since = now
            elif now - active_since >= DEBOUNCE_SECONDS and now - last_trigger >= COOLDOWN_SECONDS:
                trigger_upload(f"GPIO{GPIO_PIN} state={current_state}")
                last_trigger = now
                armed = False
        elif not is_active:
            active_since = None
            armed = True

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
