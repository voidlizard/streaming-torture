module Main where

import Control.Monad
import Control.Monad.IO.Class
import Control.DeepSeq
import Data.ByteArray qualified as BA
import Data.ByteString.Lazy.Char8 qualified as LBS
import Data.ByteString qualified as BS
import Data.ByteString.Short qualified as SB
import Data.Function
import Data.Int
import Data.List qualified as LL
import ListT qualified as L

import Streaming.Prelude qualified as S
import Streaming qualified as S

import Crypto.Hash

import System.TimeIt

duHash :: LBS.ByteString -> SB.ShortByteString
duHash s =  SB.toShort $ BA.convert $ hash @BS.ByteString @SHA256 (LBS.toStrict s)

hugeBS :: Int64 -> Char -> LBS.ByteString
hugeBS = LBS.replicate

gig n = n * 1024*1024*1024

hugeBS_1G = hugeBS (gig 1)

byChunks :: Int64 -> LBS.ByteString -> [LBS.ByteString]
byChunks n = go []
  where
    go !acc s = case LBS.splitAt n s of
            (!pref,suff) | LBS.null pref -> acc
                         | otherwise ->  go (pref : acc) suff

unLBS f n s = case LBS.splitAt n s of
             (pref, suff) | LBS.null pref -> Nothing
                          | otherwise -> Just (f pref, suff)


unfoldLbsM :: (MonadIO m, MonadPlus m) => Int64 -> LBS.ByteString -> m (Maybe (LBS.ByteString, LBS.ByteString))
unfoldLbsM n bs = case LBS.splitAt n bs of
  (pref, suff) | LBS.null pref -> pure Nothing
               | otherwise     -> pure (Just (pref, suff))



-- ~600M
case1 :: IO ()
case1 = do
  let huge =  hugeBS (gig 10) 'A'
  let chunks = LL.unfoldr (unLBS LBS.length (256*1024)) huge
  print (sum chunks)

-- ~600M ??
case1_1 :: Int64 ->  IO ()
case1_1 n = do
  let huge =  hugeBS (gig n) 'A'
  let chunks = LL.unfoldr (unLBS (force . duHash) (256*1024)) huge
  print  $ length (force chunks)

-- ~600M, works forever
case2 :: IO ()
case2 = do
  let huge =  hugeBS (gig 10) 'A'
  pieces <- L.toList $ L.traverse (pure . LBS.length) $ L.unfoldM (unfoldLbsM (256*1024)) huge
  print (sum pieces)

-- ~700G, works ok
case2_1 :: Int64 -> IO ()
case2_1 n = do
  let huge =  hugeBS (gig n) 'A'
  pieces <- L.toList $ L.traverse (pure . force . duHash) $ L.unfoldM (unfoldLbsM (256*1024)) huge
  print  $ length (force pieces)

-- ~600M
case3 :: IO ()
case3 = do
  let huge =  hugeBS (gig 10) 'A'
  pieces <- L.toList $ fmap LBS.length $ L.unfoldM (unfoldLbsM (256*1024)) huge
  print (sum pieces)

-- ~1G, works ok
case3_1 :: Int64 -> IO ()
case3_1 n = do
  let huge =  hugeBS (gig n) 'A'
  pieces <- L.toList $ fmap (force . duHash) $ L.unfoldM (unfoldLbsM (256*1024)) huge
  print  $ length (force pieces)

fu n lbs = case LBS.splitAt n lbs of
           (pref, suff) | LBS.null pref -> pure $ Left ()
                        | otherwise -> pure $ Right (pref, suff)

-- ~600M
case4 :: IO ()
case4 = do
  let huge = hugeBS (gig 10) 'A'
  qq  <- S.unfoldr (fu (256*1024)) huge
           & S.map LBS.length
           & S.toList_

  print (sum qq)


-- ~150M, slow
case4_1 :: Int64  ->  IO ()
case4_1 n = do
  let huge = hugeBS (gig n) 'A'
  qq  <- S.unfoldr (fu (256*1024)) huge
           & S.map (force . duHash)
           & S.toList_

  print  $ length (force qq)


main :: IO ()
main = do
  timeItNamed "hashes+lbs"            (case1_1 10)
  timeItNamed "hashes+list-t-slow"    (case2_1 10)
  timeItNamed "hashes+list-t-fast"    (case3_1 10)
  timeItNamed "hashes+streaming"      (case4_1 10)

