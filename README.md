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
    - [Frame ID Mapping](#frame-id-mapping)
    - [Drop Messages](#drop-messages)
    - [Topic Times](#topic-times)
      - [Update the ROS header time](#update-the-ros-header-time)
      - [Change ROS Timestamp to publish time](#change-ros-timestamp-to-publish-time)
      - [Change MCAP pub/log time from ROS header](#change-mcap-publog-time-from-ros-header)
      - [Add a time offset to ROS Timestamp](#add-a-time-offset-to-ros-timestamp)
    - [Pointcloud](#pointcloud)
      - [Remove zero points from PointCloud2](#remove-zero-points-from-pointcloud2)
      - [Rotate a pointcloud](#rotate-a-pointcloud)
      - [Remove ego bounding box points from the pointcloud](#remove-ego-bounding-box-points-from-the-pointcloud)
      - [Rename PointCloud2 field name](#rename-pointcloud2-field-name)
    - [TF](#tf)
    - [Remove Transform](#remove-transform)
      - [Insert Static Transform](#insert-static-transform)
      - [Apply Transform Offsets](#apply-transform-offsets)
    - [Schema Mapping](#schema-mapping)
    - [Trim](#trim)
    - [Plugins](#plugins)
    - [ROS1 to ROS2 conversion](#ros1-to-ros2-conversion)
    - [Reproducibility](#reproducibility)
  - [Cut](#cut)
    - [Split on time](#split-on-time)
    - [Split on topic](#split-on-topic)

</details>

---

## Installation

`pip install kappe`

or

`uv tool install kappe`

or

`uvx kappe`

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

For complete option details use `kappe convert --help`.

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

### Frame ID Mapping

To change the `frame_id` for specific topics globally, you can use `frame_id_mapping` in your configuration:

```yaml
frame_id_mapping:
  "/imu/data": "imu_link_new"
  "/lidar/points": "lidar_frame_new"
```

### Drop Messages

To drop every nth message from a topic:

```yaml
topic:
  drop:
    /high_frequency_topic: 2 # Keep every 2nd message
    /camera/image: 10 # Keep every 10th message
```

### Topic Times

The `time_offset` config manipulates the mcap message time and/or the ROS header timestamp.
When using `default` as topic name, the config will be applied to all messages.

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

#### Remove ego bounding box points from the pointcloud

```yaml
point_cloud:
  /sensor/points:
    ego_bounds:
      x:
        min: -1.0
        max: 2.0
      y:
        min: -0.5
        max: 0.5
      z:
        min: -0.2
        max: 1.5
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

Removes transforms from `/tf` and `/tf_static` messages where the child_frame_id matches the specified values.

**For /tf messages:**

```yaml
tf:
  remove:
    - test_data_frame
    - other_frame
```

**For /tf_static messages:**

```yaml
tf_static:
  remove:
    - test_data_frame
    - other_frame
```

Or remove all transforms by using the string "all":

```yaml
tf_static:
  remove: all
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

#### Apply Transform Offsets

Apply translation and rotation offsets to existing transforms in both `/tf` and `/tf_static` messages. The offsets are added to the current transform values.

> Rotation can be specified in `euler_deg` or `quaternion`

**For /tf messages:**

```yaml
tf:
  offset:
    - child_frame_id: sensor_frame
      translation:
        x: 0.1
        y: -0.05
        z: 0.2
      rotation:
        euler_deg:
          - 0
          - 0
          - 45
```

**For /tf_static messages:**

```yaml
tf_static:
  offset:
    - child_frame_id: sensor_frame
      translation:
        x: 0.1
        y: -0.05
        z: 0.2
      rotation:
        euler_deg:
          - 0
          - 0
          - 45

    - child_frame_id: camera_frame
      translation:
        x: -0.1
        y: 0.0
        z: 0.0
      rotation:
        quaternion:
          - 0.0
          - 0.0
          - 0.3827
          - 0.9239
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

Kappe can be extended with plugins, for example to compress images or update camera calibration. Source code for plugins can be found in [src/kappe/plugins/](./src/kappe/plugins/), additional plugins can be loaded from `./plugins`.

**Available built-in plugins:**

- `image.CompressImage` - Compress RGB images to JPEG
- `image.CropImage` - Crop images to specified bounds
- `camera_info.UpdateCameraInfo` - Update camera calibration parameters
- `camera_info.InsertCameraInfo` - Insert camera calibration from image topics

```yaml
plugins:
  - name: image.CompressImage
    input_topic: /image
    output_topic: /compressed/image
    settings:
      quality: 50

  - name: camera_info.UpdateCameraInfo
    input_topic: /camera/camera_info
    output_topic: /camera/camera_info # Must remove /camera/camera_info topic (see above)
    settings:
      camera_info: # https://wiki.ros.org/camera_calibration_parsers#File_formats
        image_height: 1080
        image_width: 1920
        camera_matrix:
          rows: 3
          cols: 3
          data:
            - 1070.0691945956082
            - 0.0
            - 783.0877059808756
            - 0.0
            - 1082.911613625781
            - 544.1453400605368
            - 0.0
            - 0.0
            - 1.0
        distortion_model: plumb_bob
        distortion_coefficients:
          rows: 1
          cols: 5
          data:
            - -0.317162192616218
            - 0.09863188458267099
            - 0.009339815359941763
            - -0.000817443220874783
            - 0.0
        rectification_matrix:
          rows: 3
          cols: 3
          data:
            - 1.0
            - 0.0
            - 0.0
            - 0.0
            - 1.0
            - 0.0
            - 0.0
            - 0.0
            - 1.0
        projection_matrix:
          rows: 3
          cols: 4
          data:
            - 1070.0691945956082
            - 0.0
            - 783.0877059808756
            - 0.0
            - 0.0
            - 1082.911613625781
            - 544.1453400605368
            - 0.0
            - 0.0
            - 0.0
            - 1.0
            - 0.0
  - name: camera_info.InsertCameraInfo
    input_topic: /camera/image_raw
    output_topic: /camera/camera_info
    settings:
      camera_info:  # https://wiki.ros.org/camera_calibration_parsers#File_formats
        image_height: 1080
        image_width: 1920
        camera_matrix:
          rows: 3
          cols: 3
          data:
            - 1070.0691945956082
            - 0.0
            - 783.0877059808756
            - 0.0
            - 1082.911613625781
            - 544.1453400605368
            - 0.0
            - 0.0
            - 1.0
        distortion_model: plumb_bob
        distortion_coefficients:
          rows: 1
          cols: 5
          data:
            - -0.317162192616218
            - 0.09863188458267099
            - 0.009339815359941763
            - -0.000817443220874783
            - 0.0
        rectification_matrix:
          rows: 3
          cols: 3
          data:
            - 1.0
            - 0.0
            - 0.0
            - 0.0
            - 1.0
            - 0.0
            - 0.0
            - 0.0
            - 1.0
        projection_matrix:
          rows: 3
          cols: 4
          data:
            - 1070.0691945956082
            - 0.0
            - 783.0877059808756
            - 0.0
            - 0.0
            - 1082.911613625781
            - 544.1453400605368
            - 0.0
            - 0.0
            - 0.0
            - 1.0
            - 0.0
```

### ROS1 to ROS2 conversion

Kappe automatically converts ROS1 messages to ROS2 messages.
For ROS2 message definitions, Kappe will automatically download them from GitHub based on the specified ROS2 distribution (`ros_distro`). Supported distributions: **HUMBLE** (default), IRON (EOL), JAZZY, KILTED, ROLLING. If needed, you can still provide custom message definitions in `./msgs` folder. If the ROS2 schema name has changed use the `msg_schema.mapping` to map the old schema to the new schema.

### Reproducibility

Kappe saves the input/output path, the time and the version into a MCAP metadata field, called `convert_metadata`.
The config will be saved as an attachment named `convert_config.yaml`.

## Cut

`kappe cut [-h] [--config CONFIG] [--overwrite] mcap [output]`

Cuts a mcap file into smaller mcaps, based on timestamp or topic.

For complete option details use `kappe cut --help`.

When `keep_tf_tree` is set to `true` all splits will have the same `/tf_static` messages.

### Split on time

The start and end times define the range to extract into each split file.
They are specified in seconds which is compared against the log time (UNIX Timestamp).

```yaml
keep_tf_tree: true
splits:
  - start: 1676549454.0
    end: 1676549554.0
    name: beginning.mcap
  - start: 1676549554.0
    end: 1676549654.0
    name: end.mcap
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
