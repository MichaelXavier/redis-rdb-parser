module Main
    ( main
    ) where

-------------------------------------------------------------------------------
import           Control.Applicative
import           Control.Monad.State
import           Data.Attoparsec.ByteString
import qualified Data.ByteString            as BS
import           Debug.Trace
import           Test.Tasty
import           Test.Tasty.HUnit
-------------------------------------------------------------------------------
import           Database.Redis.RDB
-------------------------------------------------------------------------------


main :: IO ()
main = defaultMain testSuite


-------------------------------------------------------------------------------
testSuite :: TestTree
testSuite = testGroup "redis-rdb-parser"
  [
     testCase "parses an example file" $ do
       bs <- BS.readFile "test/data/example.rdb"
       print (parseOnly rdbP bs)
       assertFailure "todo test"
  ]


-------------------------------------------------------------------------------
rdbP :: Parser (RDBMagicNumber, RDBVersionNumber, [(RDBDatabaseSelector, KVPair)], RCRC64, EOF)
rdbP = do
  mn <- rdbMagicNumberP
  vn <- traceShow ("mn", mn) rdbVersionNumberP
  kvps <- many (trace "try left" (Left <$> rdbDatabaseSelectorP) <|> trace "tryRight" (Right <$> kvPairP))
  eof <- eofP
  crc <- checksumP
  return (mn, vn, kvState kvps, crc, eof)


-------------------------------------------------------------------------------
kvState :: [Either RDBDatabaseSelector KVPair] -> [(RDBDatabaseSelector, KVPair)]
kvState eithers = concat $ flip evalState (RDBDatabaseSelector 0) $ forM eithers $ \e -> do
  case e of
    Left sel -> do
      put sel
      return mempty
    Right kvp -> do
      sel <- get
      return [(sel, kvp)]
