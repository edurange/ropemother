# Contributing

This repository is maintained primarily for development of `ropemother` and its use in eduRange research, teaching, and related software. If you find a problem, please start by opening an issue. Please discuss substantial changes before opening a pull request; the library is still changing, and review capacity is limited.

Exercise curriculum and exercise-specific application code are maintained separately at:

<https://github.com/edurange/ropemother-exercises>

## Development checks

Install the development dependencies into the environment used for `ropemother` development:

```sh
python -m pip install -e ".[dev]"
```

Python changes should be formatted with Black and checked with Pylint using the configuration in `pyproject.toml`. Run the tests and the smallest relevant executable demonstration for the behavior being changed.

Testing and CI/CD are still developing. New work should not depend on automation that is not yet present in the repository.

## Public interfaces and exercises

Changes to public `ropemother` interfaces can affect the exercise repository even when the library change is internally small. When a change affects interfaces used by the exercises, check the corresponding participant and support code or record the required follow-up there.

Ordinary application-facing interfaces should continue to hide bus-internal machinery such as compact IDs, symbol registration records, raw capture records, transport frames, and internal helper wiring unless that machinery is deliberately exposed as a supported extension point.

## License

By submitting a contribution to this repository, you agree that it may be distributed under the MIT License used by this project.
