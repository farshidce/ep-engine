/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"
#include "vbucket.hh"
#include "checkpoint.hh"

void Checkpoint::setState(checkpoint_state state) {
    checkpointState = state;
    // If this checkpoint is closed, clear the key index.
    if (checkpointState != opened) {
        keyIndex.clear();
    }
}

queue_dirty_t Checkpoint::queueDirty(queued_item item, CheckpointManager *checkpointManager) {
    assert (checkpointState == opened);

    uint64_t newMutationId = ++(checkpointManager->mutationCounter);
    queue_dirty_t rv;
    checkpoint_index::iterator it = keyIndex.find(item->getKey());
    // Check if this checkpoint already had an item for the same key.
    if (it != keyIndex.end()) {
        std::list<queued_item>::iterator currPos = it->second.position;
        uint64_t currMutationId = it->second.mutation_id;

        if (*(checkpointManager->persistenceCursor.currentCheckpoint) == this) {
            // If the existing item is in the left-hand side of the item pointed by the
            // persistence cursor, decrease the persistence cursor's offset by 1.
            std::string key = (*(checkpointManager->persistenceCursor.currentPos))->getKey();
            checkpoint_index::iterator ita = keyIndex.find(key);
            uint64_t mutationId = ita->second.mutation_id;
            if (currMutationId <= mutationId) {
                --(checkpointManager->persistenceCursorOffset);
            }
            // If the persistence cursor points to the existing item for the same key,
            // shift the cursor left by 1.
            if (checkpointManager->persistenceCursor.currentPos == currPos) {
                --(checkpointManager->persistenceCursor.currentPos);
            }
        }

        std::map<const std::string, CheckpointCursor>::iterator map_it;
        for (map_it = checkpointManager->tapCursors.begin();
             map_it != checkpointManager->tapCursors.end(); map_it++) {
            // If an TAP cursor points to the existing item for the same key, shift it left by 1
            if (*(map_it->second.currentCheckpoint) == this &&
                map_it->second.currentPos == currPos) {
                --(map_it->second.currentPos);
            }
        }
        // Copy the queued time of the existing item to the new one.
        item->setQueuedTime((*currPos)->getQueuedTime());
        // Remove the existing item for the same key from the list.
        toWrite.erase(currPos);
        rv = EXISTING_ITEM;
    } else {
        ++numItems;
        rv = NEW_ITEM;
    }
    // Push the new item into the list
    toWrite.push_back(item);

    std::list<queued_item>::iterator last = toWrite.end();
    // --last is okay as the list is not empty now.
    index_entry entry = {--last, newMutationId};
    // Set the index of the key to the new item that is pushed back into the list.
    keyIndex[item->getKey()] = entry;
    return rv;
}

Atomic<rel_time_t> CheckpointManager::checkpointPeriod = 5;
Atomic<size_t> CheckpointManager::checkpointMaxItems = 100000;

CheckpointManager::~CheckpointManager() {
    LockHolder lh(queueLock);
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    while(it != checkpointList.end()) {
        delete *it;
        ++it;
    }
}

uint64_t CheckpointManager::getOpenCheckpointId() {
    LockHolder lh(queueLock);
    if (checkpointList.size() == 0) {
        return 0;
    }
    return checkpointList.back()->getId();
}

void CheckpointManager::setOpenCheckpointId(uint64_t id) {
    LockHolder lh(queueLock);
    if (checkpointList.size() > 0) {
        checkpointList.back()->setId(id);
    }
}

void CheckpointManager::addNewCheckpoint_UNLOCKED(uint64_t id) {
    Checkpoint *checkpoint = new Checkpoint(id, opened);
    // Add a dummy item into the new checkpoint, so that any cursor referring to the actual first
    // item in this new checkpoint can be safely shifted left by 1 if the first item is removed
    // and pushed into the tail.
    queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
    checkpoint->queueDirty(qi, this);
    checkpointList.push_back(checkpoint);
}

void CheckpointManager::addNewCheckpoint(uint64_t id) {
    LockHolder lh(queueLock);
    addNewCheckpoint_UNLOCKED(id);
}

void CheckpointManager::registerPersistenceCursor() {
    LockHolder lh(queueLock);
    assert(checkpointList.size() > 0);
    persistenceCursor.currentCheckpoint = checkpointList.begin();
    persistenceCursor.currentPos = checkpointList.front()->begin();
    checkpointList.front()->incrReferenceCounter();
}

bool CheckpointManager::registerTAPCursor(const std::string &name, uint64_t checkpointId) {
    LockHolder lh(queueLock);
    if (checkpointList.size() == 0) {
        return false;
    }

    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); it++) {
        if (checkpointId <= (*it)->getId()) {
            break;
        }
    }
    if (it == checkpointList.end()) {
        return false;
    }

    CheckpointCursor cursor(it, (*it)->begin());
    tapCursors[name] = cursor;
    (*it)->incrReferenceCounter();
    return true;
}

bool CheckpointManager::removeTAPCursor(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        return false;
    }
    (*(it->second.currentCheckpoint))->decrReferenceCounter();

    tapCursors.erase(it);
    return true;
}

size_t CheckpointManager::getNumOfTAPCursors() {
    LockHolder lh(queueLock);
    return tapCursors.size();
}

uint64_t CheckpointManager::removeClosedUnrefCheckpoints(std::set<queued_item,
                                                                  CompareQueuedItemsByKey> &items) {

    // This function is executed periodically by the non-IO dispatcher.
    LockHolder lh(queueLock);
    std::list<Checkpoint*> unrefCheckpointList;
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    for (; it != checkpointList.end(); it++) {
        if ((*it)->getReferenceCounter() > 0) {
            break;
        }
    }
    unrefCheckpointList.splice(unrefCheckpointList.begin(), checkpointList,
                               checkpointList.begin(), it);
    lh.unlock();

    if (unrefCheckpointList.size() == 0) {
        return 0;
    }

    std::list<Checkpoint*>::reverse_iterator chkpoint_it = unrefCheckpointList.rbegin();
    uint64_t checkpoint_id = (*chkpoint_it)->getId();
    // Traverse the list of unreferenced checkpoints in the reverse order.
    for (; chkpoint_it != unrefCheckpointList.rend(); chkpoint_it++) {
        std::list<queued_item>::iterator list_it = (*chkpoint_it)->begin();
        for (; list_it != (*chkpoint_it)->end(); list_it++) {
            items.insert(*list_it);
        }
        delete *chkpoint_it;
    }
    return checkpoint_id;
}

void CheckpointManager::queueDirty(queued_item item, const RCPtr<VBucket> &vbucket) {
    LockHolder lh(queueLock);
    // The current open checkpoint should be always the last one in the checkpoint list.
    assert(checkpointList.back()->getState() == opened);
    if (checkpointList.back()->queueDirty(item, this) == NEW_ITEM) {
        ++numItems;
    }

    assert(vbucket);
    if (vbucket->getState() == vbucket_state_active) {
        // Create the new open checkpoint if the time elapsed since the creation of the current
        // checkpoint is greater than the threshold or it is reached to the max number of mutations.
        if (checkpointList.back()->getNumItems() > checkpointMaxItems ||
            (checkpointList.back()->getNumItems() > 0 &&
             (ep_current_time() - checkpointList.back()->getCreationTime()) >= checkpointPeriod)) {

            checkpointList.back()->setState(closed);
            addNewCheckpoint_UNLOCKED(nextCheckpointId++);
        }
    }
    // Note that the creation of a new checkpoint on the replica vbucket will be controlled by TAP
    // mutation messages from the active vbucket, which contain the checkpoint Ids.
}

uint64_t CheckpointManager::getAllItemsFromCurrentPosition(CheckpointCursor &cursor,
                                                           std::vector<queued_item> &items) {
    while (true) {
        while (++(cursor.currentPos) != (*(cursor.currentCheckpoint))->end()) {
            items.push_back(*(cursor.currentPos));
        }
        if ((*(cursor.currentCheckpoint))->getState() == closed) {
            // Decrease the reference count of the current checkpoint by 1.
            (*(cursor.currentCheckpoint))->decrReferenceCounter();
            // Move the cursor to the next checkpoint.
            ++(cursor.currentCheckpoint);
            cursor.currentPos = (*(cursor.currentCheckpoint))->begin();
            // Increase the reference counter of the next checkpoint by 1.
            (*(cursor.currentCheckpoint))->incrReferenceCounter();
        } else { // The cursor is currently in the open checkpoint and reached to
                 // the end() of the open checkpoint.
            --(cursor.currentPos);
            break;
        }
    }

    uint64_t checkpointId = 0;
    // Get the last closed checkpoint Id.
    if(checkpointList.size() > 1) {
        std::list<Checkpoint*>::iterator it = cursor.currentCheckpoint;
        --it;
        checkpointId = (*it)->getId();
    }

    return checkpointId;
}

uint64_t CheckpointManager::getAllItemsForPersistence(std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    uint64_t checkpointId = getAllItemsFromCurrentPosition(persistenceCursor, items);
    persistenceCursorOffset += items.size();
    return checkpointId;
}

uint64_t CheckpointManager::getAllItemsForTAPConnection(const std::string &name,
                                                    std::vector<queued_item> &items) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "The cursor for TAP connection \"%s\" is not found in the checkpoint.\n",
                         name.c_str());
        return 0;
    }

    return getAllItemsFromCurrentPosition(it->second, items);
}

queued_item CheckpointManager::nextItem(const std::string &name) {
    LockHolder lh(queueLock);
    std::map<const std::string, CheckpointCursor>::iterator it = tapCursors.find(name);
    if (it == tapCursors.end()) {
        getLogger()->log(EXTENSION_LOG_DEBUG, NULL,
                         "The cursor for TAP connection \"%s\" is not found in the checkpoint.\n",
                         name.c_str());
        queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
        return qi;
    }

    CheckpointCursor &cursor = it->second;
    if ((*(it->second.currentCheckpoint))->getState() == closed) {
        return nextItemFromClosedCheckpoint(cursor);
    } else {
        return nextItemFromOpenedCheckpoint(cursor);
    }
}

queued_item CheckpointManager::nextItemFromClosedCheckpoint(CheckpointCursor &cursor) {
    ++(cursor.currentPos);
    if (cursor.currentPos != (*(cursor.currentCheckpoint))->end()) {
        return *(cursor.currentPos);
    } else {
        // decr the reference counter for the current checkpoint by 1.
        (*(cursor.currentCheckpoint))->decrReferenceCounter();
        // Move the cursor to the next checkpoint.
        ++(cursor.currentCheckpoint);
        cursor.currentPos = (*(cursor.currentCheckpoint))->begin();
        // incr the reference counter for the next checkpoint by 1.
        (*(cursor.currentCheckpoint))->incrReferenceCounter();

        if ((*(cursor.currentCheckpoint))->getState() == closed) { // the close checkpoint.
            ++(cursor.currentPos); // Move the cursor to point to the actual first item.
            return *(cursor.currentPos);
        } else { // the open checkpoint.
            return nextItemFromOpenedCheckpoint(cursor);
        }
    }
}

queued_item CheckpointManager::nextItemFromOpenedCheckpoint(CheckpointCursor &cursor) {
    ++(cursor.currentPos);
    if (cursor.currentPos != (*(cursor.currentCheckpoint))->end()) {
        return *(cursor.currentPos);
    } else {
        --(cursor.currentPos);
        queued_item qi(new QueuedItem("", 0xffff, queue_op_empty));
        return qi;
    }
}

void CheckpointManager::clear() {
    LockHolder lh(queueLock);
    std::list<Checkpoint*>::iterator it = checkpointList.begin();
    // Remove all the checkpoints.
    while(it != checkpointList.end()) {
        delete *it;
        ++it;
    }
    checkpointList.clear();
    // Add a new open checkpoint.
    addNewCheckpoint_UNLOCKED(nextCheckpointId++);

    // Reset the persistence cursor.
    persistenceCursor.currentCheckpoint = checkpointList.begin();
    persistenceCursor.currentPos = checkpointList.front()->begin();
    checkpointList.front()->incrReferenceCounter();

    // Reset all the persistence cursors.
    std::map<const std::string, CheckpointCursor>::iterator cit = tapCursors.begin();
    for (; cit != tapCursors.end(); ++cit) {
        cit->second.currentCheckpoint = checkpointList.begin();
        cit->second.currentPos = checkpointList.front()->begin();
        checkpointList.front()->incrReferenceCounter();
    }

    numItems = 0;
    persistenceCursorOffset = 0;
    mutationCounter = 0;
}
