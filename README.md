# Kappe

Kappe is an efficient data migration tool designed to seamlessly convert and split MCAP files.

[![PyPI version](https://img.shields.io/pypi/v/kappe.svg)](https://pypi.python.org/pypi/kappe/)
[![PyPI license](https://img.shields.io/pypi/l/kappe.svg)](https://pypi.python.org/pypi/kappe/)
[![PyPI download month](https://img.shields.io/pypi/dm/kappe.svg)](https://pypi.python.org/pypi/kappe/)

<details>
<summary>Table of content</summary>

## Table of content

- [Kappe](#kappe)
  - [Table of content](#table-of-content)
  - [Installation](#installation)
    - [Usage](#usage)
  - [Convert](#convert)
    - [Topic](#topic)
      - [Rename a topic](#rename-a-topic)
      - [Remove a topic](#remove-a-topic)
      - [Update the ROS header time](#update-the-ros-header-time)
      - [Change ROS Timestamp to publish time](#change-ros-timestamp-to-publish-time)
      - [Change MCAP pub/log time from ROS header](#change-mcap-publog-time-from-ros-header)
      - [Add a time offset to ROS Timestamp](#add-a-time-offset-to-ros-timestamp)
    - [Pointcloud](#pointcloud)
      - [Remove zero points from PointCloud2](#remove-zero-points-from-pointcloud2)
      - [Rotate a pointcloud](#rotate-a-pointcloud)
      - [Rename PointCloud2 field name](#rename-pointcloud2-field-name)
    - [TF](#tf)
    - [Remove Transform](#remove-transform)
      - [Insert Static Transform](#insert-static-transform)
    - [Schema Mapping](#schema-mapping)
    - [Trim](#trim)
    - [Plugins](#plugins)
    - [ROS1 to ROS2 conversion](#ros1-to-ros2-conversion)
    - [Reproducibility](#reproducibility)
  - [Cut](#cut)
    - [Split on time](#split-on-time)
    - [Split on topic](#split-on-topic)

</details>

------

## Installation

`pip install kappe`

or

`rye tools install kappe`

### Usage

Create a yaml config file containing the migrations you want to perform.

Example:

> config.yaml

```yaml
topic:
  mapping:
    /points: /sensor/points
```

Run the converter:

`kappe convert --config config.yaml ./input.mcap`

## Convert

`kappe convert [-h] [--config CONFIG] [--overwrite] input output`

Converts a single file or a directory of files to the MCAP format.

### Topic

#### Rename a topic

```yaml
topic:
  mapping:
    /points: /sensor/points
```

#### Remove a topic

```yaml
topic:
  remove:
    - /points
```

#### Update the ROS header time

Adds 8 second and 300 nanosec to the ROS header.

```yaml
time_offset:
  /sensor/points:
    sec: 8
    nanosec: 300
```

#### Change ROS Timestamp to publish time

Change the time of the ROS Timestamp to the time the message was published.

```yaml
time_offset:
  /sensor/points:
    pub_time: True
```

#### Change MCAP pub/log time from ROS header

Update the log/pub time from the ROS header.
If `pub_time` is set, pub time will be used as source.
If `sec` and/or `nanosec` is set, the offset is used.

```yaml
time_offset:
  /sensor/points:
    update_publish_time: True
    update_log_time: True
```

#### Add a time offset to ROS Timestamp

Add 15 seconds to the ROS Timestamp.

```yaml
time_offset:
  /sensor/points:
    sec: 15
    nanosec: 0
```

### Pointcloud

#### Remove zero points from PointCloud2

```yaml
point_cloud:
  /sensor/points:
    remove_zero: true
```

#### Rotate a pointcloud

```yaml
point_cloud:
  /sensor/points:
    rotation:
      euler_deg:
        - 180
        - 0
        - 0
```

#### Rename PointCloud2 field name

Changes the field name of the PointCloud2 message, from `AzimuthAngle` to `azimuth_angle`.

```yaml
point_cloud:
  /sensor/points:
    field_mapping:
      AzimuthAngle: azimuth_angle
```

### TF

> To update a static transform you need to remove the old one and insert a new one.

### Remove Transform

Removes all transform where the child_frame_id is `test_data_frame` or `other_frame`.

```yaml
tf_static:
  remove:
    - test_data_frame
    - other_frame
```

#### Insert Static Transform

> Rotation can be specified in `euler_deg` or `quaternion`

```yaml
tf_static:
  insert:
    - frame_id: base
      child_frame_id: also_base

    - frame_id: base
      child_frame_id: sensor
      translation:
        x: -0.1
        y: 0
        z: 0.1
      rotation:
        euler_deg:
          - 0
          - 90
          - 0
```

### Schema Mapping

If the new schema is not already in the mcap, kappe will try to load it either from your ROS2 environment or from `./msgs`.

```yaml
msg_schema:
  mapping:
    std_msgs/Int32: std_msgs/Int64
```

### Trim

Trim the mcap file to a specific time range.

```yaml
time_start:  1676549454.0
time_end:    1676549554.0
```

### Plugins

Kappe can be extended with plugins, for example to compress images. Source code for plugins can be found in the [plugins](./src/kappe/), additional plugins can be loaded from `./plugins`.

```yaml
plugins:
  - name: image.CompressImage
    input_topic: /image
    output_topic: /compressed/image
    settings:
      quality: 50
```

### ROS1 to ROS2 conversion

Kappe automatic converts ROS1 messages to ROS2 messages.
It will not reuse ROS1 definitions, all schemas will be loaded either from your ROS2 environment or from `./msgs`. If the ROS2 schema name has changed use the `msg_schema.mapping` to map the old schema to the new schema.

The msg field will be mapped exactly, ROS1 time and duration will be converted to ROS2 time and duration (`secs -> sec` & `nsecs -> nanosec`).

To download the common ROS2 schemas run:

```sh
git clone --depth=1 --branch=humble https://github.com/ros2/common_interfaces.git ./msgs/common_interfaces
git clone --depth=1 --branch=humble https://github.com/ros2/geometry2.git ./msgs/geometry2
```

### Reproducibility

Kappe saves the input/output path, the time and the version into a MCAP metadata field, called `convert_metadata`.
The config will be saved as an attachment named `convert_config.yaml`.

## Cut

`usage: kappe cut [-h] --config CONFIG [--overwrite] input [output_folder]`

Cuts a directory of mcaps into smaller mcaps, based on timestamp or topic.

When `keep_tf_tree` is set to `true` all splits will have the same `/tf_static` messages.

### Split on time

```yaml
keep_tf_tree: true
splits:
  - start:  1676549454.0
    end:    1676549554.0
    name:   beginning.mcap
  - start:  1676549554.0
    end:    1676549654.0
    name:   end.mcap
```

`kappe cut --config config.yaml ./input.mcap ./output_folder`

Results in a folder with the following structure:

```bash
output_folder
├── beginning.mcap
└── end.mcap
```

### Split on topic

Splits the mcap file into multiple files, every time a message on the topic `/marker` is read.
The file will be split **before** the message is read.
`debounce` is the time in seconds that the cutter will wait before splitting the file again, default is 0.

```yaml
keep_tf_tree: true
split_on_topic:
  topic: "/marker"
  debounce: 10

```
