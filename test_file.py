# 1. Создай новую ветку от текущей
git checkout -b feature/deduplication

# 2. Добавь файлы
git add .

# 3. Закоммить
git commit -m "Добавить дедупликацию транзакций с TTL 72 часа"

# 4. Запушь
git push -u origin feature/deduplication
