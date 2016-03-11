#ifndef _STREAM_FLUSHER_H
#define _STREAM_FLUSHER_H

template<typename S>
struct StreamFlusher
{
    StreamFlusher(S& stream): mStream(stream) {}
    ~StreamFlusher() { mStream.flush(); }

    S& mStream;
};
#endif

