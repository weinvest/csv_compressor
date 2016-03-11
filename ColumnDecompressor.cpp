#include  <boost/lexical_cast.hpp>
#include <boost/iostreams/filter/bzip2.hpp>
#include <boost/iostreams/device/file.hpp>
#include <boost/algorithm/string/split.hpp>
#include "ColumnDecompressor.h"
#include "StreamFlusher.h"
ColumnDecompressor::ColumnDecompressor(char delimiter, size_t nCacheSize)
    :mCacheSize(nCacheSize)
    ,mDelimiter(delimiter)
    ,mCachePool(nullptr)
{
    mNewLine[0] = '\n';
}

ColumnDecompressor::~ColumnDecompressor()
{
    delete mCachePool;
}

void ColumnDecompressor::RowCache::WriteOut(bio::filtering_ostream& output, std::array<size_t, MAX_COLUMNS>& columnIndices, size_t nTotalColumns)
{
    for(size_t iColumn = 0; iColumn < nTotalColumns; ++iColumn)
    {
        size_t index = columnIndices[iColumn];
        size_t size = Fields[index + 1] - Fields[index];
        output.write(Fields[index], size);
    }
}

namespace bal = boost::algorithm;
void ColumnDecompressor::Decompress(const std::string& inputFileName, const std::string& outputFileName)
{
    bio::filtering_istream inputStream;
    inputStream.push(bio::bzip2_decompressor());
    inputStream.push(bio::file_source(inputFileName));

    //decompress header
    std::string header;
    std::getline(inputStream, header);

    std::vector<boost::iterator_range<std::string::iterator>> columns;
    bal::split(columns, header, [this](auto c) { return c == mDelimiter; }, bal::token_compress_off);

    bio::filtering_ostream outputStream;
    outputStream.push(bio::file_sink(outputFileName));
    StreamFlusher<bio::filtering_ostream> flusher(outputStream);
    outputStream << header << mNewLine[0];

    //decompress meta
    size_t nTotalColumns = columns.size();
    std::string sMeta;
    if(!std::getline(inputStream, sMeta))
    {
        return;
    }

    columns.clear();
    bal::split(columns, sMeta, [this](auto c) {return c == mDelimiter;}, bal::token_compress_off);
    std::array<size_t, MAX_COLUMNS> columnIndices;
    std::array<std::pair<size_t, size_t> , MAX_COLUMNS> metas;
    for(size_t iColumn = 0; iColumn < columns.size(); ++iColumn)
    {
        std::string sColumn = std::string(*columns[iColumn].begin(), columns[iColumn].size());
        metas[iColumn].first = boost::lexical_cast<size_t>(sColumn);
        metas[iColumn].second = iColumn;
    }

    std::sort(metas.begin(), metas.end(), [this](auto& l, auto& r){ return l.first < r.first; });
    for(size_t iColumn = 0; iColumn < columns.size(); ++iColumn)
    {
        columnIndices[iColumn] = metas[iColumn].second;
    }

    mCachePool = new char[mCacheSize];

    //compress others
    std::string column;
    size_t nCurrentColumn = 0;
    while(std::getline(inputStream, column))
    {
        ++nCurrentColumn;
        if(nCurrentColumn == nTotalColumns)
        {
            nCurrentColumn = 0;
            WriteOutCache(outputStream, columnIndices, nTotalColumns);
        }
    }

    WriteOutCache(outputStream, columnIndices, nTotalColumns);
}

void ColumnDecompressor::WriteOutCache(bio::filtering_ostream& output, std::array<size_t, MAX_COLUMNS>& columnIndices, size_t nTotalColumns)
{
    for(auto& rowCache : mRows)
    {
        rowCache->WriteOut(output, columnIndices, nTotalColumns);
    }
}
