# Cookbook

## Cutting an MCAP

> config.yaml

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
