set -e

mkdir -p sqlite-amalgamation
cd sqlite-amalgamation

curl -O https://sqlite.com/2024/sqlite-amalgamation-3450100.zip
unzip sqlite-amalgamation-3450100.zip
mv sqlite-amalgamation-3450100/* .

rm -rf sqlite-amalgamation-3450100.zip
rm -rf sqlite-amalgamation-3450100

# enable R-Tree
LINE='#define SQLITE_ENABLE_RTREE 1'
sed -i '' "1s|^|$LINE\\n|" sqlite3.c

cd ..
npm install better-sqlite3 --no-save --build-from-source --sqlite3="$(pwd)/sqlite-amalgamation"