#include <arrow/api.h>
#include <arrow/filesystem/api.h>
#include <arrow/json/api.h>
#include <arrow/memory_pool.h>

#include <iostream>

int main() {
  arrow::Status st;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  auto fs =
      arrow::fs::FileSystemFromUri("file:///home/kabbe/Code/CPP/arrow_json")
          .ValueOrDie();
  auto file = fs->OpenInputFile("/home/kabbe/Code/CPP/arrow_json/test.json")
                  .ValueOrDie();

  auto read_options = arrow::json::ReadOptions::Defaults();
  auto parse_options = arrow::json::ParseOptions::Defaults();

  // Instantiate TableReader from input stream and options
  // std::shared_ptr<arrow::json::TableReader> reader;
  auto reader =
      arrow::json::TableReader::Make(pool, file, read_options, parse_options)
          .ValueOrDie();

  // Read table from JSON file
  auto table = reader->Read().ValueOrDie();

  auto b = std::static_pointer_cast<arrow::StructArray>(
      table->GetColumnByName("b")->chunk(0));

  std::cout << "b: " << b->ToString() << "\n";
  auto x = b->GetFieldByName("x");
  if (x) {
    auto val = std::static_pointer_cast<arrow::Int64Array>(x);
    if (val) {
      std::cout << "val\n";
      std::cout << "val: " << val->ToString() << "\n";
    }
  }
}