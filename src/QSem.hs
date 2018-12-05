{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE GADTSyntax #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Simple quantity semaphores.
module QSem
  ( -- * Simple Quantity Semaphores
    QSem       -- abstract
  , newQSem    -- :: Int -> IO QSem
  , waitQSem   -- :: QSem -> IO ()
  , signalQSem -- :: QSem -> IO ()
  ) where

import GHC.Prim (MVar#, RealWorld)
import Control.Concurrent.MVar
import GHC.MVar
import GHC.IO
import Prelude hiding (reverse)

data MVarList where
  MNil :: MVarList
  MCons :: !(MVar# RealWorld ()) -> MVarList -> MVarList

reverse :: MVarList -> MVarList
reverse l = rev l MNil
  where
    rev MNil a = a
    rev (MCons x xs) a = rev xs (MCons x a)

-- The semaphore state (i, xs, ys):
--
--   i is the current resource value
--
--   (xs,ys) is the queue of blocked threads, where the queue is
--           given by xs ++ reverse ys.  We can enqueue new blocked threads
--           by consing onto ys, and dequeue by removing from the head of xs.
--
data SemaphoreState = SS
  { _currentResourceValue :: {-# UNPACK #-} !Int
  , _queueForward :: !(MVarList)
  , _queueReverse :: !(MVarList)
  }

-- | 'QSem' is a quantity semaphore in which the resource is acquired
-- and released in units of one. It provides guaranteed FIFO ordering
-- for satisfying blocked `waitQSem` calls.
--
-- The pattern
--
-- >   bracket_ waitQSem signalQSem (...)
--
-- is safe; it never loses a unit of the resource.
newtype QSem = QSem (MVar SemaphoreState)

-- A blocked thread is represented by an empty (MVar ()).  To unblock
-- the thread, we put () into the MVar.
--
-- A thread can dequeue itself by also putting () into the MVar, which
-- it must do if it receives an exception while blocked in waitQSem.
-- This means that when unblocking a thread in signalQSem we must
-- first check whether the MVar is already full; the MVar lock on the
-- semaphore itself resolves race conditions between signalQSem and a
-- thread attempting to dequeue itself.

-- |  Build a new 'QSem' with a supplied initial quantity.
--    The initial quantity must be at least 0.
newQSem :: Int -> IO QSem
newQSem !initial
  | initial < 0 = fail "newQSem: Initial quantity must be non-negative"
  | otherwise = do
      sem <- newMVar $ SS initial MNil MNil
      return (QSem sem)

-- | Wait for a unit to become available.
waitQSem :: QSem -> IO ()
waitQSem (QSem !m) = mask_ $ do
  (SS i b1 b2) <- takeMVar m 
  if i == 0
    then do
      (MVar b) <- newEmptyMVar
      putMVar m (SS i b1 (MCons b b2))
      wait b
    else do
      let !z = i - 1
      putMVar m (SS z b1 b2)
      return ()
  where
    wait :: MVar# RealWorld () -> IO ()
    wait b = takeMVar# b `onException` do
      uninterruptibleMask_ $ do
        (SS i b1 b2) <- takeMVar m
        r <- tryTakeMVar (MVar b)
        r' <- case r of { Just _ -> signal (SS i b1 b2); Nothing -> do { putMVar (MVar b) (); return (SS i b1 b2) } }
        putMVar m r'

-- | Signal that a unit of the 'QSem' is available
signalQSem :: QSem -> IO ()
signalQSem (QSem !m) = uninterruptibleMask_ $ do
  r <- takeMVar m
  r' <- signal r
  putMVar m r'

--   Note [signal uninterruptible]
--
--   If we have
--
--      bracket waitQSem signalQSem (...)
--
--   and an exception arrives at the signalQSem, then we must not lose
--   the resource.  The signalQSem is masked by bracket, but taking
--   the MVar might block, and so it would be interruptible.  Hence we
--   need an uninterruptibleMask here.
--
--   This isn't ideal: during high contention, some threads won't be
--   interruptible.  The QSemSTM implementation has better behaviour
--   here, but it performs much worse than this one in some
--   benchmarks.
signal :: SemaphoreState -> IO SemaphoreState
signal (SS i a1 a2) =
  if i == 0
    then loop a1 a2
    else let !z = i + 1 in return (SS z a2 a2)
  where
    loop MNil MNil = return (SS 1 MNil MNil)
    loop MNil b2   = loop (reverse b2) MNil
    loop (MCons b bs) b2 = do
      r <- tryPutMVar (MVar b) ()
      if r then return (SS 0 bs b2)
           else loop bs b2

takeMVar# :: MVar# RealWorld a -> IO a
{-# INLINE takeMVar# #-}
takeMVar# m = takeMVar (MVar m)