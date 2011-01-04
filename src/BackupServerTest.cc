/* Copyright (c) 2009 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include <cstring>

#include "TestUtil.h"
#include "BackupServer.h"
#include "BindTransport.h"
#include "CoordinatorServer.h"
#include "Log.h"
#include "Logging.h"
#include "MasterServer.h"
#include "MockTransport.h"
#include "RecoverySegmentIterator.h"
#include "Rpc.h"
#include "SegmentIterator.h"
#include "TransportManager.h"

namespace RAMCloud {

/**
 * Unit tests for BackupServer.
 */
class BackupServerTest : public CppUnit::TestFixture {

    CPPUNIT_TEST_SUITE(BackupServerTest);
    CPPUNIT_TEST(test_closeSegment);
    CPPUNIT_TEST(test_closeSegment_segmentNotOpen);
    CPPUNIT_TEST(test_closeSegment_segmentClosed);
    CPPUNIT_TEST(test_findSegmentInfo);
    CPPUNIT_TEST(test_findSegmentInfo_notIn);
    CPPUNIT_TEST(test_freeSegment);
    CPPUNIT_TEST(test_freeSegment_stillOpen);
    CPPUNIT_TEST(test_getRecoveryData);
    CPPUNIT_TEST(test_getRecoveryData_moreThanOneSegmentStored);
    CPPUNIT_TEST(test_getRecoveryData_malformedSegment);
    CPPUNIT_TEST(test_getRecoveryData_notRecovered);
    CPPUNIT_TEST(test_openSegment);
    CPPUNIT_TEST(test_openSegment_alreadyOpen);
    CPPUNIT_TEST(test_openSegment_outOfStorage);
    CPPUNIT_TEST(test_startReadingData);
    CPPUNIT_TEST(test_startReadingData_empty);
    CPPUNIT_TEST(test_writeSegment);
    CPPUNIT_TEST(test_writeSegment_segmentNotOpen);
    CPPUNIT_TEST(test_writeSegment_segmentClosed);
    CPPUNIT_TEST(test_writeSegment_badOffset);
    CPPUNIT_TEST(test_writeSegment_badLength);
    CPPUNIT_TEST(test_writeSegment_badOffsetPlusLength);
    CPPUNIT_TEST_SUITE_END();

    BackupServer* backup;
    BackupClient* client;
    CoordinatorServer* coordinatorServer;
    const uint32_t segmentSize;
    const uint32_t segmentFrames;
    BackupStorage* storage;
    BackupServer::Config* config;
    BindTransport* transport;

  public:
    BackupServerTest()
        : backup(NULL)
        , client(NULL)
        , coordinatorServer(NULL)
        , segmentSize(1 << 10)
        , segmentFrames(2)
        , storage(NULL)
        , config(NULL)
        , transport(NULL)
    {
        logger.setLogLevels(SILENT_LOG_LEVEL);
    }

    void
    setUp()
    {
        logger.setLogLevels(SILENT_LOG_LEVEL);
        config = new BackupServer::Config();
        config->coordinatorLocator = "mock:host=coordinator";
        storage = new InMemoryStorage(segmentSize, segmentFrames);

        transport = new BindTransport();
        transportManager.registerMock(transport);
        coordinatorServer = new CoordinatorServer();
        transport->addServer(*coordinatorServer, "mock:host=coordinator");
        backup = new BackupServer(*config, *storage);
        transport->addServer(*backup, "mock:host=backup");
        client =
            new BackupClient(transportManager.getSession("mock:host=backup"));
    }

    void
    tearDown()
    {
        delete client;
        delete backup;
        delete coordinatorServer;
        transportManager.unregisterMock();
        delete transport;
        delete storage;
        delete config;
        CPPUNIT_ASSERT_EQUAL(0,
            BackupStorage::Handle::resetAllocatedHandlesCount());
    }

    uint32_t
    writeEntry(uint64_t masterId, uint64_t segmentId, LogEntryType type,
               uint32_t offset, const void *data, uint32_t bytes)
    {
        SegmentEntry entry;
        entry.type = type;
        entry.length = bytes;
        client->writeSegment(masterId, segmentId,
                             offset, &entry, sizeof(entry));
        client->writeSegment(masterId, segmentId, offset + sizeof(entry),
                             data, bytes);
        return sizeof(entry) + bytes;
    }

    uint32_t
    writeObject(uint64_t masterId, uint64_t segmentId,
                uint32_t offset, const char *data, uint32_t bytes,
                uint64_t tableId, uint64_t objectId)
    {
        char objectMem[sizeof(Object) + bytes];
        Object* obj = reinterpret_cast<Object*>(objectMem);
        obj->id = objectId;
        obj->table = tableId;
        obj->version = 0;
        obj->checksum = 0xff00ff00ff00;
        obj->data_len = bytes;
        memcpy(objectMem + sizeof(*obj), data, bytes);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_OBJ, offset,
                          objectMem, sizeof(Object) + bytes);
    }

    uint32_t
    writeTombstone(uint64_t masterId, uint64_t segmentId,
                   uint32_t offset, uint64_t tableId, uint64_t objectId)
    {
        ObjectTombstone tombstone(segmentId, tableId, objectId, 0);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_OBJTOMB, offset,
                          &tombstone, sizeof(tombstone));
    }

    uint32_t
    writeHeader(uint64_t masterId, uint64_t segmentId)
    {
        SegmentHeader header;
        header.logId = masterId;
        header.segmentId = segmentId;
        header.segmentCapacity = segmentSize;
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_SEGHEADER, 0,
                          &header, sizeof(header));
    }

    uint32_t
    writeFooter(uint64_t masterId, uint64_t segmentId, uint32_t offset)
    {
        SegmentFooter footer;
        footer.checksum =
            static_cast<Segment::Checksum::ResultType>(0xff00ff00ff00);
        return writeEntry(masterId, segmentId, LOG_ENTRY_TYPE_SEGFOOTER, offset,
                          &footer, sizeof(footer));
    }

    void
    test_closeSegment()
    {
        client->openSegment(99, 88);
        client->writeSegment(99, 88, 10, "test", 4);
        client->closeSegment(99, 88);
        BackupServer::SegmentInfo &info = *backup->findSegmentInfo(99, 88);
        char* storageAddress =
            static_cast<InMemoryStorage::Handle*>(info.storageHandle)->
                getAddress();
        CPPUNIT_ASSERT(NULL != storageAddress);
        CPPUNIT_ASSERT_EQUAL("test", &storageAddress[10]);
        CPPUNIT_ASSERT_EQUAL(NULL, static_cast<void*>(info.segment));
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_closeSegment_segmentNotOpen()
    {
        CPPUNIT_ASSERT_THROW(client->closeSegment(99, 88),
                             BackupBadSegmentIdException);
    }

    void
    test_closeSegment_segmentClosed()
    {
        client->openSegment(99, 88);
        client->closeSegment(99, 88);
        CPPUNIT_ASSERT_THROW(client->closeSegment(99, 88),
                             BackupBadSegmentIdException);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_findSegmentInfo()
    {
        CPPUNIT_ASSERT_EQUAL(NULL, backup->findSegmentInfo(99, 88));
        client->openSegment(99, 88);
        client->closeSegment(99, 88);
        BackupServer::SegmentInfo* infop = backup->findSegmentInfo(99, 88);
        CPPUNIT_ASSERT(infop != NULL);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_findSegmentInfo_notIn()
    {
        CPPUNIT_ASSERT_EQUAL(NULL, backup->findSegmentInfo(99, 88));
    }

    static bool
    inMemoryStorageFreePred(string s)
    {
        return s == "free";
    }

    void
    test_freeSegment()
    {
        client->openSegment(99, 88);
        client->writeSegment(99, 88, 10, "test", 4);
        client->closeSegment(99, 88);
        {
            TestLog::Enable _(&inMemoryStorageFreePred);
            client->freeSegment(99, 88);
            CPPUNIT_ASSERT_EQUAL("free: called", TestLog::get());
        }
        CPPUNIT_ASSERT_EQUAL(NULL, backup->findSegmentInfo(99, 88));
    }

    void
    test_freeSegment_stillOpen()
    {
        client->openSegment(99, 88);
        client->freeSegment(99, 88);
        CPPUNIT_ASSERT_EQUAL(NULL, backup->findSegmentInfo(99, 88));
    }

    void
    appendTablet(ProtoBuf::Tablets& tablets,
                 uint64_t partitionId,
                 uint32_t tableId,
                 uint64_t start, uint64_t end)
    {
        ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
        tablet.set_table_id(tableId);
        tablet.set_start_object_id(start);
        tablet.set_end_object_id(end);
        tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
        tablet.set_user_data(partitionId);
    }

    void
    createTabletList(ProtoBuf::Tablets& tablets)
    {
        // partition 0
        appendTablet(tablets, 0, 123, 0, 9);
        appendTablet(tablets, 0, 123, 10, 19);
        appendTablet(tablets, 0, 123, 20, 29);
        appendTablet(tablets, 0, 124, 20, 100);

        // partition 1
        appendTablet(tablets, 1, 123, 30, 39);
        appendTablet(tablets, 1, 125, 0, std::numeric_limits<uint64_t>::max());
    }

    void
    test_getRecoveryData()
    {
        ProtoBuf::Tablets tablets;
        createTabletList(tablets);

        uint32_t offset = 0;
        client->openSegment(99, 88);
        offset = writeHeader(99, 88);
        // Objects
        // Barely in tablet
        offset += writeObject(99, 88, offset, "test1", 6, 123, 29);
        // Barely out of tablets
        offset += writeObject(99, 88, offset, "test2", 6, 123, 30);
        // In on other table
        offset += writeObject(99, 88, offset, "test3", 6, 124, 20);
        // Not in any table
        offset += writeObject(99, 88, offset, "test4", 6, 125, 20);
        // Tombstones
        // Barely in tablet
        offset += writeTombstone(99, 88, offset, 123, 29);
        // Barely out of tablets
        offset += writeTombstone(99, 88, offset, 123, 30);
        // In on other table
        offset += writeTombstone(99, 88, offset, 124, 20);
        // Not in any table
        offset += writeTombstone(99, 88, offset, 125, 20);
        offset += writeFooter(99, 88, offset);
        client->closeSegment(99, 88);
        client->startReadingData(99, tablets);

        Buffer response;
        BackupClient::GetRecoveryData(*client, 99, 88, 0, response)();

        RecoverySegmentIterator it(
            response.getRange(0, response.getTotalLength()),
            response.getTotalLength());

        CPPUNIT_ASSERT(!it.isDone());
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, it.getType());
        CPPUNIT_ASSERT_EQUAL(123, it.get<Object>()->table);
        CPPUNIT_ASSERT_EQUAL(29, it.get<Object>()->id);
        it.next();

        CPPUNIT_ASSERT(!it.isDone());
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, it.getType());
        CPPUNIT_ASSERT_EQUAL(124, it.get<Object>()->table);
        CPPUNIT_ASSERT_EQUAL(20, it.get<Object>()->id);
        it.next();

        CPPUNIT_ASSERT(!it.isDone());
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
        CPPUNIT_ASSERT_EQUAL(123, it.get<ObjectTombstone>()->tableId);
        CPPUNIT_ASSERT_EQUAL(29, it.get<ObjectTombstone>()->objectId);
        it.next();

        CPPUNIT_ASSERT(!it.isDone());
        CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJTOMB, it.getType());
        CPPUNIT_ASSERT_EQUAL(124, it.get<ObjectTombstone>()->tableId);
        CPPUNIT_ASSERT_EQUAL(20, it.get<ObjectTombstone>()->objectId);
        it.next();

        CPPUNIT_ASSERT(it.isDone());
    }

    void
    test_getRecoveryData_moreThanOneSegmentStored()
    {
        uint32_t offset = 0;
        client->openSegment(99, 87);
        offset = writeHeader(99, 87);
        offset += writeObject(99, 87, offset, "test1", 6, 123, 9);
        offset += writeFooter(99, 87, offset);
        client->closeSegment(99, 87);

        client->openSegment(99, 88);
        offset = writeHeader(99, 88);
        offset += writeObject(99, 88, offset, "test2", 6, 123, 10);
        offset += writeFooter(99, 88, offset);
        client->closeSegment(99, 88);

        ProtoBuf::Tablets tablets;
        createTabletList(tablets);

        client->startReadingData(99, tablets);

        {
            Buffer response;
            BackupClient::GetRecoveryData(*client, 99, 88, 0, response)();

            RecoverySegmentIterator it(
                response.getRange(0, response.getTotalLength()),
                response.getTotalLength());
            CPPUNIT_ASSERT(!it.isDone());
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, it.getType());
            CPPUNIT_ASSERT_EQUAL("test2",
                                 static_cast<const Object*>(it.getPointer())->
                                    data);
            it.next();
            CPPUNIT_ASSERT(it.isDone());
        }
        {
            Buffer response;
            BackupClient::GetRecoveryData(*client, 99, 87, 0, response)();

            RecoverySegmentIterator it(
                response.getRange(0, response.getTotalLength()),
                response.getTotalLength());
            CPPUNIT_ASSERT(!it.isDone());
            CPPUNIT_ASSERT_EQUAL(LOG_ENTRY_TYPE_OBJ, it.getType());
            CPPUNIT_ASSERT_EQUAL("test1",
                                 static_cast<const Object*>(it.getPointer())->
                                    data);
            it.next();
            CPPUNIT_ASSERT(it.isDone());
        }

        client->freeSegment(99, 87);
        client->freeSegment(99, 88);
    }

    void
    test_getRecoveryData_malformedSegment()
    {
        client->openSegment(99, 88);
        client->closeSegment(99, 88);

        client->startReadingData(99, ProtoBuf::Tablets());
        Buffer response;

        BackupClient::GetRecoveryData cont(*client, 99, 88,
                                           0, response);
        logger.setLogLevels(SILENT_LOG_LEVEL);
        CPPUNIT_ASSERT_THROW(cont(), SegmentRecoveryFailedException);

        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_getRecoveryData_notRecovered()
    {
        uint32_t offset = 0;
        client->openSegment(99, 88);
        offset += writeHeader(99, 88);
        offset += writeObject(99, 88, offset, "test2", 6, 123, 10);
        offset += writeFooter(99, 88, offset);
        Buffer response;

        BackupClient::GetRecoveryData cont(*client, 99, 88,
                                           0, response);
        CPPUNIT_ASSERT_THROW(cont(),
                             BackupBadSegmentIdException);

        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_openSegment()
    {
        client->openSegment(99, 88);
        BackupServer::SegmentInfo &info = *backup->findSegmentInfo(99, 88);
        CPPUNIT_ASSERT(NULL != info.segment);
        CPPUNIT_ASSERT_EQUAL(0, *info.segment);
        char* address =
            static_cast<InMemoryStorage::Handle*>(info.storageHandle)->
                getAddress();
        CPPUNIT_ASSERT(NULL != address);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_openSegment_alreadyOpen()
    {
        client->openSegment(99, 88);
        CPPUNIT_ASSERT_THROW(
            client->openSegment(99, 88),
            BackupSegmentAlreadyOpenException);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_openSegment_outOfStorage()
    {
        client->openSegment(99, 86);
        client->openSegment(99, 87);
        CPPUNIT_ASSERT_THROW(
            client->openSegment(99, 88),
            BackupStorageException);
        CPPUNIT_ASSERT_EQUAL(2,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_startReadingData()
    {
        client->openSegment(99, 88);
        vector<uint64_t> result = client->startReadingData(99,
                                                           ProtoBuf::Tablets());
        CPPUNIT_ASSERT_EQUAL(1, result.size());
        CPPUNIT_ASSERT_EQUAL(88, result[0]);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_startReadingData_empty()
    {
        vector<uint64_t> result = client->startReadingData(99,
                                                           ProtoBuf::Tablets());
        CPPUNIT_ASSERT_EQUAL(0, result.size());
    }

    void
    test_writeSegment()
    {
        client->openSegment(99, 88);
        client->writeSegment(99, 88, 10, "test", 4);
        BackupServer::SegmentInfo &info = *backup->findSegmentInfo(99, 88);
        CPPUNIT_ASSERT(NULL != info.segment);
        CPPUNIT_ASSERT_EQUAL("test", &info.segment[10]);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_writeSegment_segmentNotOpen()
    {
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 0, "test", 4),
            BackupBadSegmentIdException);
    }

    void
    test_writeSegment_segmentClosed()
    {
        client->openSegment(99, 88);
        client->closeSegment(99, 88);
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 0, "test", 4),
            BackupBadSegmentIdException);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_writeSegment_badOffset()
    {
        client->openSegment(99, 88);
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 500000, "test", 0),
            BackupSegmentOverflowException);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_writeSegment_badLength()
    {
        client->openSegment(99, 88);
        char junk[70000];
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 0, junk, sizeof(junk)),
            BackupSegmentOverflowException);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

    void
    test_writeSegment_badOffsetPlusLength()
    {
        client->openSegment(99, 88);
        char junk[50000];
        CPPUNIT_ASSERT_THROW(
            client->writeSegment(99, 88, 50000, junk, 50000),
            BackupSegmentOverflowException);
        CPPUNIT_ASSERT_EQUAL(1,
            BackupStorage::Handle::getAllocatedHandlesCount());
    }

  private:
    DISALLOW_COPY_AND_ASSIGN(BackupServerTest);
};
CPPUNIT_TEST_SUITE_REGISTRATION(BackupServerTest);

class SegmentInfoTest : public ::testing::Test {
  public:
    typedef BackupServer::SegmentInfo SegmentInfo;
    typedef BackupStorage::Handle Handle;
    SegmentInfoTest()
        : segmentSize(64 * 1024)
        , pool{segmentSize}
        , storage{segmentSize, 2}
        , info{storage, pool, 99, 88}
    {
        logger.setLogLevels(SILENT_LOG_LEVEL);
    }

    uint32_t segmentSize;
    BackupServer::Pool pool;
    InMemoryStorage storage;
    SegmentInfo info;

};

TEST_F(SegmentInfoTest, destructor) {
    TestLog::Enable _;
    {
        SegmentInfo info{storage, pool, 99, 88};
        info.open();
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    }
    EXPECT_EQ("~SegmentInfo: Backup shutting down with open segment <99,88>, "
              "closing out to storage", TestLog::get());
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
    ASSERT_EQ(static_cast<char*>(NULL), info.segment);
}

TEST_F(SegmentInfoTest, destructorLoading) {
    {
        SegmentInfo info{storage, pool, 99, 88};
        info.open();
        EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
        info.close();
        info.startLoading();
    }
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
}

void
appendTablet(ProtoBuf::Tablets& tablets,
             uint64_t partitionId,
             uint32_t tableId,
             uint64_t start, uint64_t end)
{
    ProtoBuf::Tablets::Tablet& tablet(*tablets.add_tablet());
    tablet.set_table_id(tableId);
    tablet.set_start_object_id(start);
    tablet.set_end_object_id(end);
    tablet.set_state(ProtoBuf::Tablets::Tablet::RECOVERING);
    tablet.set_user_data(partitionId);
}

void
createTabletList(ProtoBuf::Tablets& tablets)
{
    appendTablet(tablets, 0, 123, 0, 9);
    appendTablet(tablets, 0, 123, 10, 19);
    appendTablet(tablets, 0, 123, 20, 29);
    appendTablet(tablets, 0, 124, 20, 100);
    appendTablet(tablets, 1, 123, 30, 39);
    appendTablet(tablets, 1, 125, 0, std::numeric_limits<uint64_t>::max());
}

TEST_F(SegmentInfoTest, appendRecoverySegment) {
    info.open();
    Segment segment(123, 88, info.getSegment(), segmentSize);

    SegmentHeader header = { 99, 88, segmentSize };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    Object object(sizeof(object));
    object.id = 10;
    object.table = 123;
    object.version = 0;
    object.checksum = 0xff00ff00ff00;
    object.data_len = 0;
    segment.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));

    segment.close();
    info.close();
    info.setRecovering();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions, segmentSize);

    Buffer buffer;
    info.appendRecoverySegment(0, buffer);
    RecoverySegmentIterator it(buffer.getRange(0, buffer.getTotalLength()),
                                 buffer.getTotalLength());
    EXPECT_FALSE(it.isDone());
    EXPECT_EQ(LOG_ENTRY_TYPE_OBJ, it.getType());
    EXPECT_EQ(sizeof(Object), it.getLength());

    it.next();
    EXPECT_TRUE(it.isDone());
}

TEST_F(SegmentInfoTest, appendRecoverySegmentMalformedSegment) {
    info.open();
    memcpy(info.getSegment(), "garbage", 7);
    info.setRecovering();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions, segmentSize);

    Buffer buffer;
    EXPECT_THROW(info.appendRecoverySegment(0, buffer),
                 SegmentRecoveryFailedException);
}

TEST_F(SegmentInfoTest, appendRecoverySegmentNotYetRecovered) {
    Buffer buffer;
    TestLog::Enable _;
    EXPECT_THROW(info.appendRecoverySegment(0, buffer),
                 BackupBadSegmentIdException);
    EXPECT_EQ("appendRecoverySegment: Asked for segment <99,88> which wasn't "
              "recovered yet", TestLog::get());
}

TEST_F(SegmentInfoTest, appendRecoverySegmentPartitionOutOfBounds) {
    info.open();
    Segment segment(123, 88, info.getSegment(), segmentSize);
    segment.close();
    info.close();
    info.setRecovering();
    ProtoBuf::Tablets partitions;
    info.buildRecoverySegments(partitions, segmentSize);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
    Buffer buffer;
    TestLog::Enable _;
    EXPECT_THROW(info.appendRecoverySegment(0, buffer),
                 BackupBadSegmentIdException);
    EXPECT_EQ("appendRecoverySegment: Asked for recovery segment 0 from "
              "segment <99,88> but there are only 0 partitions",
              TestLog::get());
}

TEST_F(SegmentInfoTest, whichPartition) {
    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    Object object(sizeof(object));
    object.id = 10;
    object.table = 123;
    object.version = 0;
    object.checksum = 0xff00ff00ff00;
    object.data_len = 0;

    auto r = whichPartition(LOG_ENTRY_TYPE_OBJ, &object, partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(0u, *r);

    object.id = 30;
    r = whichPartition(LOG_ENTRY_TYPE_OBJ, &object, partitions);
    EXPECT_TRUE(r);
    EXPECT_EQ(1u, *r);

    TestLog::Enable _;
    object.id = 40;
    r = whichPartition(LOG_ENTRY_TYPE_OBJ, &object, partitions);
    EXPECT_FALSE(r);
    EXPECT_EQ("whichPartition: Couldn't place object <123,40> into any of the "
              "given tablets for recovery; hopefully it belonged to a deleted "
              "tablet or lives in another log now", TestLog::get());
}

TEST_F(SegmentInfoTest, buildRecoverySegment) {
    info.open();
    Segment segment(123, 88, info.getSegment(), segmentSize);

    SegmentHeader header = { 99, 88, segmentSize };
    segment.append(LOG_ENTRY_TYPE_SEGHEADER, &header, sizeof(header));

    Object object(sizeof(object));
    object.id = 10;
    object.table = 123;
    object.version = 0;
    object.checksum = 0xff00ff00ff00;
    object.data_len = 0;
    segment.append(LOG_ENTRY_TYPE_OBJ, &object, sizeof(object));

    segment.close();
    info.close();
    info.setRecovering();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions, segmentSize);

    EXPECT_FALSE(info.recoveryException);
    EXPECT_EQ(2u, info.recoverySegmentsLength);
    ASSERT_TRUE(info.recoverySegments);
    EXPECT_EQ(sizeof(object) + sizeof(SegmentEntry),
              info.recoverySegments[0].getTotalLength());
    EXPECT_EQ(0u,
              info.recoverySegments[1].getTotalLength());
}

TEST_F(SegmentInfoTest, buildRecoverySegmentMalformedSegment) {
    info.open();
    memcpy(info.getSegment(), "garbage", 7);
    info.setRecovering();

    ProtoBuf::Tablets partitions;
    createTabletList(partitions);

    info.buildRecoverySegments(partitions, segmentSize);
    EXPECT_TRUE(info.recoveryException);
    EXPECT_FALSE(info.recoverySegments);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
}

TEST_F(SegmentInfoTest, buildRecoverySegmentNoTablets) {
    info.open();
    Segment segment(123, 88, info.getSegment(), segmentSize);
    segment.close();
    info.setRecovering();
    info.buildRecoverySegments(ProtoBuf::Tablets(), segmentSize);
    EXPECT_FALSE(info.recoveryException);
    EXPECT_EQ(0u, info.recoverySegmentsLength);
    ASSERT_TRUE(info.recoverySegments);
}

TEST_F(SegmentInfoTest, close) {
    info.open();
    EXPECT_EQ(SegmentInfo::OPEN, info.state);
    ASSERT_TRUE(pool.is_from(info.segment));
    const char* magic = "kitties!";
    snprintf(info.segment, segmentSize, "%s", magic);

    info.close();
    EXPECT_EQ(SegmentInfo::CLOSED, info.state);
    EXPECT_FALSE(pool.is_from(info.segment));

    char seg[segmentSize];
    storage.getSegment(info.storageHandle, seg);
    EXPECT_STREQ(magic, seg);

    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
}

TEST_F(SegmentInfoTest, closeWhileNotOpen) {
    EXPECT_THROW(info.close(), BackupBadSegmentIdException);
}

TEST_F(SegmentInfoTest, free) {
    info.open();
    info.close();
    info.startLoading();
    EXPECT_TRUE(pool.is_from(info.segment));
    EXPECT_TRUE(info.isLoading());
    EXPECT_FALSE(info.inMemory());
    EXPECT_EQ(1, BackupStorage::Handle::getAllocatedHandlesCount());
    info.free();
    EXPECT_FALSE(pool.is_from(info.segment));
    EXPECT_FALSE(info.isLoading());
    EXPECT_EQ(0, BackupStorage::Handle::getAllocatedHandlesCount());
    EXPECT_EQ(SegmentInfo::FREED, info.state);
}

TEST_F(SegmentInfoTest, getSegment) {
    info.open();
    EXPECT_TRUE(pool.is_from(info.segment));
    const char* magic = "kitties!";
    snprintf(info.segment, segmentSize, "%s", magic);
    info.close();
}

TEST_F(SegmentInfoTest, open) {
    info.open();
    ASSERT_NE(static_cast<char*>(NULL), info.segment);
    EXPECT_EQ('\0', info.segment[0]);
    EXPECT_NE(static_cast<Handle*>(NULL), info.storageHandle);
    EXPECT_EQ(SegmentInfo::OPEN, info.state);
}

TEST_F(SegmentInfoTest, openStorageAllocationFailure) {
    InMemoryStorage storage{segmentSize, 0};
    SegmentInfo info{storage, pool, 99, 88};
    EXPECT_THROW(info.open(), BackupStorageException);
    ASSERT_EQ(static_cast<char*>(NULL), info.segment);
    EXPECT_EQ(static_cast<Handle*>(NULL), info.storageHandle);
    EXPECT_EQ(SegmentInfo::UNINIT, info.state);
}

TEST_F(SegmentInfoTest, startLoading) {
    info.open();
    info.close();
    info.startLoading();
    ASSERT_NE(static_cast<char*>(NULL), info.segment);
    EXPECT_TRUE(info.isLoading());
    EXPECT_EQ(SegmentInfo::CLOSED, info.state);
}

} // namespace RAMCloud
