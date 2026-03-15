#!/bin/bash

# Рендеринг PlantUML диаграмм в PNG
# Использование: ./scripts/render-diagrams.sh [папка]
# По умолчанию: schemas/
# Формат: PLANTUML_FORMAT=svg ./scripts/render-diagrams.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SCHEMAS_DIR="${1:-$PROJECT_ROOT/schemas}"
FORMAT="${PLANTUML_FORMAT:-png}"

if ! command -v plantuml &>/dev/null; then
    echo "Ошибка: plantuml не найден. Установите: brew install plantuml"
    exit 1
fi

echo "PlantUML: $SCHEMAS_DIR -> $FORMAT"
echo "---"

count=0
while IFS= read -r -d '' puml; do
    dir="$(dirname "$puml")"
    rel_path="${puml#$PROJECT_ROOT/}"
    name="$(basename "$puml" .puml)"
    images_dir="$dir/images"
    mkdir -p "$images_dir"

    (cd "$dir" && plantuml -t"$FORMAT" -o "$images_dir" "$(basename "$puml")")
    echo "  $rel_path -> images/${name}.${FORMAT}"
    ((count++)) || true
done < <(find "$SCHEMAS_DIR" -name "*.puml" -type f -print0)

echo "---"
echo "Готово: $count диаграмм"
