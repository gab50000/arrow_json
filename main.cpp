#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/json/api.h>
#include <arrow/memory_pool.h>

#include <iostream>

int main() {
  // Copied from https://arrow.apache.org/docs/cpp/json.html#basic-usage
  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto fs =
      arrow::fs::FileSystemFromUri("file:///home/kabbe/Code/CPP/arrow_json")
          .ValueOrDie();
  auto file = fs->OpenInputFile("/home/kabbe/Code/CPP/arrow_json/test.json")
                  .ValueOrDie();

  auto read_options = arrow::json::ReadOptions::Defaults();
  auto parse_options = arrow::json::ParseOptions::Defaults();

  auto reader =
      arrow::json::TableReader::Make(pool, file, read_options, parse_options)
          .ValueOrDie();

  auto table = reader->Read().ValueOrDie();
  std::cout << "table schema: " << table->schema()->ToString() << "\n";
  /*
    This shows: table schema: b: list<item: struct<x: int64>>
  */

  auto vec_of_chunks = table->GetColumnByName("b")->chunks();

  for (const auto& chunk : vec_of_chunks) {
    auto b_list = std::static_pointer_cast<arrow::ListArray>(chunk);

    auto b_struct = std::static_pointer_cast<arrow::StructArray>(
        b_list->Flatten().ValueOrDie());

    auto x_arr = std::static_pointer_cast<arrow::Int64Array>(
        b_struct->GetFieldByName("x"));

    for (int i = 0; i < x_arr->length(); i++) {
      auto x = x_arr->Value(i);
      std::cout << "x = " << x << "\n";
    }
  }
}
