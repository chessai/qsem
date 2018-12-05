{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UnboxedTuples #-}
{-# LANGUAGE GADTSyntax #-}

-- | Quantity semaphores in which each thread may wait for an arbitrary \"amount\".
module QSemN
  ( -- * General Quantity Semaphores
    QSemN       -- abstract
  , newQSemN    -- :: Int -> IO QSemN
  , waitQSemN   -- :: QSemN -> Int -> IO ()
  , signalQSemN -- :: QSemN -> Int -> IO ()
  ) where

import GHC.Prim (MVar#, RealWorld)
import Control.Concurrent.MVar
import GHC.MVar
import GHC.IO
import Prelude hiding (reverse)

type QuantityMVar = (# Int, MVar# RealWorld () #)

data MVarList where
  MNil  :: MVarList
  MCons :: !QuantityMVar -> MVarList -> MVarList

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

-- | 'QSemN' is a quantity semaphore in which the resource is acquired
--   and released in units of one. It provides guaranteed FIFO ordering
--   for satisfying blocked `waitQSemN` calls.
--
--   The pattern
--
--   >   bracket_ (waitQSemN n) (signalQSemN n) (...)
--
--   is safe; it never loses any of the resource.
newtype QSemN = QSemN (MVar SemaphoreState)

-- A blocked thread is represented by an empty (MVar ()).  To unblock
-- the thread, we put () into the MVar.
--
-- A thread can dequeue itself by also putting () into the MVar, which
-- it must do if it receives an exception while blocked in waitQSemN.
-- This means that when unblocking a thread in signalQSemN we must
-- first check whether the MVar is already full; the MVar lock on the
-- semaphore itself resolves race conditions between signalQSemN and a
-- thread attempting to dequeue itself.

-- | Build a new 'QSemN' with a supplied initial quantity.
--   The initial quantity must be at least 0.
newQSemN :: Int -> IO QSemN
newQSemN !initial
  | initial < 0 = fail "newQSemN: Initial quantity must be non-negative"
  | otherwise = do
      sem <- newMVar (SS initial MNil MNil)
      return (QSemN sem)

-- | Wait for the specified quantity to become available.
waitQSemN :: QSemN -> Int -> IO ()
waitQSemN (QSemN !m) !sz = mask_ $ do
  (SS i b1 b2) <- takeMVar m
  let !z = i - sz
  if z < 0
    then do
      bl@(MVar b) <- newEmptyMVar
      putMVar m (SS i b1 (MCons (# sz, b #) b2))
      wait bl
    else do
      putMVar m (SS z b1 b2)
      return ()
  where
    wait :: MVar () -> IO () 
    wait b = takeMVar b `onException`
      ( uninterruptibleMask_ $ do
          ss <- takeMVar m
          r <- tryTakeMVar b
          r' <- case r of { Just _ -> signal sz ss; Nothing -> do { putMVar b (); return ss; } }
          putMVar m r'
      )

-- | Signal that a given quantity is now available from the 'QSemN'.
signalQSemN :: QSemN -> Int -> IO ()
signalQSemN (QSemN !m) !sz = uninterruptibleMask_ $ do
  r <- takeMVar m
  r' <- signal sz r
  putMVar m r'

signal :: Int -> SemaphoreState -> IO SemaphoreState
signal !sz0 (SS i a1 a2) = loop (sz0 + i) a1 a2
  where
    loop 0 bs b2 = return (SS 0 bs b2)
    loop sz MNil MNil = return (SS sz MNil MNil)
    loop sz MNil b2 = loop sz (reverse b2) MNil
    loop sz jbbs@(MCons (# j, b #) bs) b2
      | j > sz = do
          r <- isEmptyMVar (MVar b)
          if r then return (SS sz jbbs b2)
               else loop sz bs b2
      | otherwise = do
          r <- tryPutMVar (MVar b) ()
          if r then loop (sz - j) bs b2
               else loop sz bs b2
