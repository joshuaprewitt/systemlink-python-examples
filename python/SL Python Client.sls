ni-systemlink-server_18-5_released:
  pkgrepo.managed:
  - name: ni-systemlink-server_18-5_released
  - uri: "https://download.ni.com/support/nipkg/products/ni-s/ni-systemlink-server/18.5/released"
  - enabled: true
  - compressed: false
ni-systemlink-server_18-5_released-critical:
  pkgrepo.managed:
  - name: ni-systemlink-server_18-5_released-critical
  - uri: "https://download.ni.com/support/nipkg/products/ni-s/ni-systemlink-server/18.5/released-critical"
  - enabled: true
  - compressed: false
packages:
  pkg.installed:
  - pkgs:
    - ni-systemlink-python-3.7
    - ni-systemlink-python-3.7-systemlink-sdk
'C:\Program Files\National Instruments\Shared\Skyline\Python\3.7':
  win_path.exists
    