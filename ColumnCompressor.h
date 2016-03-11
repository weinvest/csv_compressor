#ifndef COLUMNCOMPRESSOR_H
#define COLUMNCOMPRESSOR_H
#include <sstream>
#include <memory>
#include <array>
#include <boost/iostreams/filtering_stream.hpp>
#include "CommonDef.h"
namespace bio = boost::iostreams;
class ColumnCompressor
{
public:
    ColumnCompressor(char delimiter, size_t nCacheSize);
    ~ColumnCompressor();

    void Compress(const std::string& inputFileName, const std::string& outputFileName);

private:
    void InitializeColumnCache(Columns& columns, size_t columnCount);
    void AdjustColumnCache(size_t columnCount);
    void CheckColumnCount(bool hasMoreData, size_t columnCount);
    bool Copy2Cache(Columns& spliters, size_t& columns);
    void WriteOutCache(bio::filtering_ostream& outputStream, size_t columnCount);
    bool Next(const char*& currentAddress,const char* endAddress, Columns& spliters, size_t& columns);

    const size_t mCacheSize;
    size_t mTotalColumns;

    size_t mTotalRows;
    size_t mCurrentRowsInBlock;

    struct ColumnCache
    {
        enum type
        {
            INT, DOUBLE, STRING
        }Type;
        size_t Index;

        char* CacheBase;
        char* CacheEnd;
        char* CacheCur;

        static type ColumnType(const char* pStart, const char* pEnd);
    };
    std::array<ColumnCache, MAX_COLUMNS> mCache;
    std::array<ColumnCache*, MAX_COLUMNS> mOutCache;

    char mDelimiter;
    char mNewLine[1];
    char* mCachePool;
};

#endif // COLUMNCOMPRESSOR_H
