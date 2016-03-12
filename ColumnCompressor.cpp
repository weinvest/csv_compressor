#include <boost/lexical_cast.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp> 
#include "ColumnCompressor.h"
#include "StreamFlusher.h"

namespace bip = boost::interprocess;
ColumnCompressor::ColumnCompressor(char delimiter, size_t nCacheSize)
    :mCacheSize(nCacheSize)
    ,mTotalColumns(0)
    ,mTotalRows(0)
    ,mCurrentRowsInBlock(0)
    ,mDelimiter(delimiter)
    ,mCachePool(nullptr)
{
}

ColumnCompressor::~ColumnCompressor()
{
    delete mCachePool;
}

void ColumnCompressor::CheckColumnCount(bool hasMoreData, size_t columnCount)
{
    if((hasMoreData && columnCount != mTotalColumns) || (!hasMoreData && columnCount > mTotalColumns))
    {
        throw std::domain_error(std::string("the column count of row 1 is "
                                            + boost::lexical_cast<std::string>(columnCount)
                                            + ", but column count of header is ")
                                + boost::lexical_cast<std::string>(mTotalColumns));
    }
}

void ColumnCompressor::Compress(const std::string& inputFileName, const std::string& outputFileName)
{
    //将输入文件映射到内存
    bip::file_mapping inputFile(inputFileName.c_str(), bip::read_only);  
    bip::mapped_region inputFileRegion(inputFile, bip::read_only);
	  
    const char* startAddress = static_cast<const char*>(inputFileRegion.get_address());  
    auto size  = inputFileRegion.get_size(); 
    const char* endAddress = startAddress + size;
    auto currentAddress = startAddress - 1;
    
    if(startAddress == endAddress)
    {
        return; //空文件
    }

    //初始化输出流
    bio::filtering_ostream outputStream;
    outputStream.push(bio::bzip2_compressor());
    outputStream.push(bio::file_sink(outputFileName));
    StreamFlusher<bio::filtering_ostream> flusher(outputStream);

    //读入header
    Columns columns;
    bool hasMoreData = Next(currentAddress, endAddress, columns, mTotalColumns);

    //写入文件头
    outputStream.write(startAddress, currentAddress - startAddress + 1);

    if(!hasMoreData)
    {
        return;
    }

    //读入第一行并p
    size_t columnCount(0);
    hasMoreData = Next(currentAddress, endAddress, columns, columnCount);
    CheckColumnCount(hasMoreData, columnCount);

    InitializeColumnCache(columns, columnCount);
    outputStream << mOutCache[0]->Index;
    for(size_t iColumn = 1; iColumn < columnCount; ++iColumn)
    {
        outputStream << "," << mOutCache[iColumn]->Index;
    }
    outputStream << '\n';
    if(!Copy2Cache(columns, columnCount))
    {
        throw std::domain_error(std::string("too small cache, please enlarge it."));
    }
    else if(!hasMoreData)
    {
        return;
    }

    //
    auto copy2Cache = [this, &outputStream](Columns& columns, size_t columnCount)
    {
        if(!Copy2Cache(columns, columnCount))
        {
            WriteOutCache(outputStream, mTotalColumns);
            Copy2Cache(columns, columnCount);
	    return true;
        }
	return false;
    };

    while(hasMoreData = Next(currentAddress, endAddress, columns, columnCount))
    {
        CheckColumnCount(hasMoreData, columnCount);
        copy2Cache(columns, columnCount);
    }

    if(!copy2Cache(columns, columnCount))
    {
        WriteOutCache(outputStream, mTotalColumns);
    }
    outputStream.flush();
}

bool ColumnCompressor::Copy2Cache(Columns& spliters, size_t columnCount)
{
    bool isFull(false);
    if(1 == columnCount && spliters[0] == (spliters[1] - 1))
    {
	//empty line
	return !isFull;
    }

    for(int32_t iColumn = 0; iColumn < columnCount; ++iColumn)
    {
        auto& columnCache = mCache[iColumn];
        auto size = spliters[iColumn + 1] - spliters[iColumn];
        auto space = columnCache.CacheEnd - columnCache.CacheCur;
        if(space < size)
        {
            isFull = true;
            //restore ColumnCache.CacheCur
            for(int32_t i = 0; i < iColumn; ++i)
            {
                auto size = spliters[i + 1] - spliters[i];
                mCache[i].CacheCur -= size;
            }
            break;
        }
        std::memcpy(columnCache.CacheCur, spliters[iColumn], size);
        columnCache.CacheCur += size;
    }
    *(mCache[columnCount - 1].CacheCur - 1)= mDelimiter;
    return !isFull;
}

void ColumnCompressor::WriteOutCache(bio::filtering_ostream& outputStream, size_t columnCount)
{
    for(int32_t iColumn = 0; iColumn < columnCount; ++iColumn)
    {
        auto size = mOutCache[iColumn]->CacheCur - mOutCache[iColumn]->CacheBase;
        outputStream.write(mOutCache[iColumn]->CacheBase, size);
        outputStream.write(NEWLINE, 1);
    }
    AdjustColumnCache(mTotalColumns);
}

void ColumnCompressor::AdjustColumnCache(size_t columnCount)
{
    std::array<double, MAX_COLUMNS> cumCacheUsed = {0};
    double cacheUsed = 0;
    for(size_t iColumn = 0; iColumn < columnCount; ++iColumn)
    {
        auto& columnCache = mCache[iColumn];
        cacheUsed += columnCache.CacheCur - columnCache.CacheBase;
        cumCacheUsed[iColumn] = cacheUsed;
    }

    if(nullptr == mCachePool)
    {
        mCachePool = new char[mCacheSize];
    }

    char* pCurr = mCachePool;
    for(size_t iColumn = 0; iColumn < columnCount; ++iColumn)
    {
        auto& columnCache = mCache[iColumn];
        columnCache.CacheCur = columnCache.CacheBase = pCurr;

        pCurr = mCachePool + (size_t)((cumCacheUsed[iColumn] / cacheUsed) * mCacheSize);
        columnCache.CacheEnd = pCurr;
    }
}

void ColumnCompressor::InitializeColumnCache(Columns& columns, size_t columnCount)
{
    for(size_t iColumn = 0; iColumn < columnCount; ++iColumn)
    {
        auto& columnCache = mCache[iColumn];
        columnCache.Type = ColumnCache::ColumnType(columns[iColumn], columns[iColumn + 1] - 1);
        columnCache.Index = iColumn;
        columnCache.CacheCur = columnCache.CacheBase + 1;

        mOutCache[iColumn] = &columnCache;
    }

    AdjustColumnCache(columnCount);

    std::sort(mOutCache.begin(), mOutCache.begin() + columnCount, [](auto l, auto r)
    {
        if(l->Type == r->Type)
        {
            return l < r;
        }
        return l->Type < r->Type;
    });
}

bool ColumnCompressor::Next(const char*& currentAddress, const char* endAddress, Columns& spliters, size_t& columnCount)
{
    bool isEmpty = true;
    while(isEmpty && (currentAddress < endAddress))
    {
        columnCount = 0;
        spliters[0] = ++currentAddress;
        while(currentAddress < endAddress && *currentAddress != NEWLINE[0])
        {
            if(*currentAddress == mDelimiter)
            {
                spliters[++columnCount] = currentAddress + 1;
            }
            ++currentAddress;
        }
	isEmpty = currentAddress == spliters[0];
    }
    spliters[++columnCount] = currentAddress + 1;
    return currentAddress < endAddress;
}

ColumnCompressor::ColumnCache::type ColumnCompressor::ColumnCache::ColumnType(const char* pStart, const char* pEnd)
{
    std::string column(pStart, pEnd - pStart);
    try
    {
        boost::lexical_cast<int32_t>(column);
        return INT;
    }
    catch(const std::exception& ex)
    {
        try
        {
            boost::lexical_cast<double>(column);
            return DOUBLE;
        }
        catch(const std::exception& ex)
        {
            return STRING;
        }
    }
}
