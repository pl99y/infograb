A static information dashboard powered by GitHub Actions and GitHub Pages.

It collects data from multiple sources, processes and translates part of the content, then exports JSON files to `docs/data/` for the frontend to render.

## Stack

- Python
- GitHub Actions
- GitHub Pages
- Gemini API

## Structure

```text
collectors/   data fetching
parsers/      parsing logic
services/     shared processing utilities
scripts/      export scripts
docs/         static frontend + exported JSON

A low-cost imitation of a well-known project in this space — partly born out of frustration with how commercial it has become.
