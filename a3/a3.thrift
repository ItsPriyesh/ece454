service KeyValueService {
  string get(1: string key);
  void put(1: string key, 2: string value);
  void backupEntry(1: string key, 2: string value, 3: i32 seq);
  void backupAll(1: list<string> keys, 2: list<string> values);
}
