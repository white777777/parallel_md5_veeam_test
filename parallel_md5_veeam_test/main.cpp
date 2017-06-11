#include <string>
#include <vector>
#include <array>
#include <list>
#include <istream>
#include <ostream>
#include <fstream>
#include <mutex>
#include <iostream>
#include <thread>
#include <sstream>
#include "md5.h"
#include <cassert>
#include <exception>
#include <future>

namespace file_hash
{
  typedef unsigned long long chunk_id;
  constexpr int DefaultChunkSize = 1024 * 1024;

  struct Chunk
  {
    chunk_id ChunkId;
    std::vector<char> Buffer;

    Chunk(size_t bufSize)
      : Buffer(bufSize)
    {}
  };

  struct MD5Hash
  {
    const static size_t hashLen = 32;
    char HashStr[hashLen];
    chunk_id ChunkId;
    MD5Hash(const Chunk& chunk)
      : ChunkId(chunk.ChunkId)
    {
      char* hashStr = _md5.digestMemory((BYTE*)chunk.Buffer.data(), chunk.Buffer.size());
      memcpy(HashStr, hashStr, hashLen);
    }
  private:
    MD5 _md5;
  };

  template <class TISTREAM>
  class ChunkStream
  {
  public:
    ChunkStream(std::shared_ptr<TISTREAM> & stream)
      : _stream(stream)
    {
    }

    bool Get(Chunk& chunk)
    {
      std::lock_guard<std::mutex> _l(_m);
      if (!_stream)
        return false;
      auto & stream = *_stream;
      chunk.ChunkId = _chunkId++;
      stream.read(chunk.Buffer.data(), chunk.Buffer.size());
      size_t dataSize = (size_t)stream.gcount();

      if (dataSize == 0)
        return false;
      chunk.Buffer.resize(dataSize);
      return true;
    }

  private:
    std::shared_ptr<TISTREAM> _stream;
    std::mutex _m;
    chunk_id _chunkId = 0;
  };

  template <class TOSTREAM>
  class ResultWriteController
  {
  public:
    ResultWriteController(std::shared_ptr<TOSTREAM> & stream, std::string lineEnding = "\r\n")
      : _stream(stream)
      , _startPos(stream->tellp())
      , _lnSize(lineEnding.size())
      , _lineEnding(lineEnding)
    {
      _stream->exceptions(std::ifstream::failbit | std::ifstream::badbit);
    }

    void Put(const MD5Hash& hash)
    {
      std::lock_guard<std::mutex> _l(_m);
      _stream->seekp(_startPos + hash.ChunkId*(hash.hashLen + _lnSize), std::ios_base::beg);
      _stream->write(hash.HashStr, hash.hashLen);
      *_stream << _lineEnding;
    }

  private:

    std::shared_ptr<TOSTREAM> _stream;
    std::mutex _m;
    std::streamoff _startPos;
    const int _lnSize = 1;
    const std::string _lineEnding;
  };


  class HashProcessor
  {
  public:
    template<class TISTREAM, class TOSTREAM>
    static void ProcessStreamHashes(std::shared_ptr<TISTREAM> inStream, std::shared_ptr<TOSTREAM> outStream, const size_t chunkSize = DefaultChunkSize)
    {
      if (chunkSize == 0)
        throw std::invalid_argument("Chunk size should be greater than 0");
      auto chunkStream = std::make_shared<ChunkStream<TISTREAM>>(inStream);
      auto resultsController = std::make_shared<ResultWriteController<TOSTREAM>>(outStream);

      auto worker = [chunkStream, resultsController, chunkSize]()
      {
        Chunk chunk(chunkSize);
        while (chunkStream->Get(chunk))
        {
          resultsController->Put(MD5Hash(chunk));
        }
      };

      unsigned nWorkers = std::thread::hardware_concurrency();
      std::list<std::future<void>> threads;

      for (size_t i = 0; i < nWorkers-1; ++i)
        threads.emplace_back(std::async(std::launch::async, worker));
      worker();
      for (std::future<void> & t : threads)
        t.get();

      if (!inStream->eof() && inStream->fail())
        throw std::runtime_error("Can't read file");
    }

    static void ProcessFileHashes(const std::string & inFilepath, const std::string & outFilepath, const size_t chunkSize = DefaultChunkSize)
    {
      ProcessStreamHashes(std::make_shared<std::ifstream>(inFilepath, std::ifstream::binary), 
        std::make_shared<std::ofstream>(outFilepath, std::ofstream::trunc|std::ofstream::binary), chunkSize);
    }
  };

  class Test
  {
  public:
    void TestFile()
    {
      HashProcessor::ProcessFileHashes("in.txt", "out.txt", 37);
    }

    void TestAll()
    {
      //VS2015 have buggy stringstream implementation, some tests may fail
      bool isExceptions = false;
      const int lnSize = 2;
      try
      {
        {
          auto inStream = std::make_shared<std::stringstream>();
          auto outStream = std::make_shared<std::stringstream>();
          HashProcessor::ProcessStreamHashes(inStream, outStream);
          std::string result = outStream->str();
          std::cout << "---" << std::endl;
          std::cout << result;

          assert(result.size() == 0);
        }

        //1 full block
        {
          auto inStream = std::make_shared<std::stringstream>();
          auto outStream = std::make_shared<std::stringstream>();
          for (int i = 0; i < DefaultChunkSize; ++i)
            *inStream << "0";
          HashProcessor::ProcessStreamHashes(inStream, outStream);
          std::string result = outStream->str();
          std::cout << "---" << std::endl;
          std::cout << result;
          assert(result.size() == MD5Hash::hashLen+lnSize);
        }

        //1 full + 1 char
        {
          auto inStream = std::make_shared<std::stringstream>();
          auto outStream = std::make_shared<std::stringstream>();
          for (int i = 0; i < DefaultChunkSize + 1; ++i)
            *inStream << "0";
          HashProcessor::ProcessStreamHashes(inStream, outStream);
          std::string result = outStream->str();
          std::cout << "---" << std::endl;
          std::cout << result;
          assert(result.size() == (MD5Hash::hashLen + lnSize)*2);
        }

        //2 full
        {
          auto inStream = std::make_shared<std::stringstream>();
          auto outStream = std::make_shared<std::stringstream>();
          for (int i = 0; i < 2* DefaultChunkSize; ++i)
            *inStream << "0";
          HashProcessor::ProcessStreamHashes(inStream, outStream);
          std::string result = outStream->str();
          std::cout << "---" << std::endl;
          std::cout << result;
          assert(result.size() == (MD5Hash::hashLen + lnSize) * 2);
          assert(result[0] == result[MD5Hash::hashLen + lnSize]);
        }

        //very small chunks
        {
          auto inStream = std::make_shared<std::stringstream>();
          auto outStream = std::make_shared<std::stringstream>();
          const int nChunks = 40;
          const int chunkSize = 1;
          for (int i = 0; i < nChunks; ++i)
            *inStream << "0";
          HashProcessor::ProcessStreamHashes(inStream, outStream, chunkSize);
          std::string result = outStream->str();
          std::cout << "---" << std::endl;
          std::cout << result;
          assert(result.size() == nChunks * (MD5Hash::hashLen + 1));
          assert(result[0] == result[MD5Hash::hashLen + 1]);
        }
      }
      catch (const std::exception & e)
      {
        std::cout << e.what() << std::endl;
        isExceptions = true;
      }

      assert(!isExceptions);
    }
  };
}

void parseArgs(char** args, int nArgs, std::string & inFile, std::string & outFile, int& blockSize)
{
  if (nArgs < 3 || nArgs > 4)
    throw std::invalid_argument("Usage: app fileIn fileOut blockSize");
  if(nArgs>=3)
  {
    inFile = std::string(args[1]);
    outFile = std::string(args[2]);
  }
  if (nArgs >= 4)
  {
    blockSize = atoi(args[3]);
  }
}

int main(char** args, int nArgs)
{
  using namespace file_hash;
  //Test().TestFile();
  return 0;
  try
  {
    std::string inFile, outFile;
    int blockSize = DefaultChunkSize;
    parseArgs(args, nArgs, inFile, outFile, blockSize);
    HashProcessor::ProcessFileHashes(inFile, outFile, blockSize);
  }
  catch(const std::exception & e)
  {
    std::cout << e.what() << std::endl;
    return -1;
  }
  return 0;
}