# MCAP kappe script

## Usage

- `kappe [-h] [--config CONFIG] [--version] [--overwrite] input output`

## Message definition for ROS1 -> ROS2 conversion

```bash
git clone --depth=1 --branch=humble https://github.com/ros2/common_interfaces.git msgs/common_interfaces
git clone --depth=1 --branch=humble https://github.com/ros2/example_interfaces.git msgs/example_interfaces
git clone --depth=1 --branch=humble https://github.com/ros2/rcl_interfaces.git msgs/rcl_interfaces
git clone --depth=1 --branch=humble https://github.com/ros2/geometry2.git msgs/geometry2
```

## Licenses

- [kappe/utils/pointcloud2.py](./kappe/utils/pointcloud2.py) based on [sensor_msgs_py/point_cloud2.py](https://github.com/ros2/common_interfaces/blob/rolling/sensor_msgs_py/sensor_msgs_py/point_cloud2.py) licensed [Apache License 2.0](https://raw.githubusercontent.com/ros2/common_interfaces/rolling/LICENSE)
