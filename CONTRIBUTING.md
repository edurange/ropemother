# Contributing

This repository contains `ropemother`, a project of the eduRange research team. If you find a problem, start by opening an issue. Please discuss changes before opening a pull request; the library is still taking shape, and review capacity is limited.

Exercise curriculum and exercise-specific application code are maintained separately at:

<https://github.com/edurange/ropemother-exercises>

## Development checks

Install the development dependencies into the environment used for `ropemother` development:

```sh
python -m pip install -e ".[dev]"
```

Format and check changed Python files with Black and Pylint using the configuration in `pyproject.toml`:

```sh
python -m black path/to/changed_file.py
python -m pylint path/to/changed_file.py
```

Testing and CI/CD are still developing.

## License

By submitting a contribution to this repository, you agree that it may be distributed under the MIT License used by this project.
