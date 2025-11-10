#!/bin/bash

# from the pyEfi repo root, with your venv activated
python3 pyefi_supervisor.py \
  --ini /path/to/mainController.ini \
  --usb /dev/ttyUSB0 \
  --channel ms2:realtime \
  --log-file /var/log/pyefi/collector.log
