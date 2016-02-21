{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.Redis.RDB
    ( -- * Types
      RDBMagicNumber(..)
    , RDBVersionNumber(..)
    , RDBDatabaseSelector(..)
    , KVPair(..)
    , RedisKey(..)
    , RedisValue(..)
    , RedisString(..)
    , RedisList(..)
    , RedisSet(..)
    , EOF(..)
    , RCRC64(..)
    -- * Parsers
    , rdbMagicNumberP
    , rdbVersionNumberP
    , rdbDatabaseSelectorP
    , kvPairP
    , checksumP
    , eofP
    -- * Trash
    , streamingParser
    , test
    , timestampP
    ) where


-------------------------------------------------------------------------------
import           Codec.Compression.LZF
import           Control.Applicative
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Resource
import           Data.Attoparsec.ByteString   as A
import           Data.Bits
import           Data.ByteString              (ByteString)
import qualified Data.ByteString              as BS
import qualified Data.ByteString.Char8        as BS8
import           Data.Conduit
import           Data.Conduit.Attoparsec
import           Data.Conduit.Binary
import qualified Data.Conduit.List            as CL
import           Data.List
import           Data.Monoid
import           Data.Ratio
import           Data.Set                     (Set)
import qualified Data.Set                     as S
import           Data.Time.Clock.POSIX
import           Data.Word
import           Debug.Trace
-------------------------------------------------------------------------------


--TODO: move elsewhere
--TODO: this is ill-conceived, we want to never apply the magic number and version number parsers after they succeed
streamingParser
    :: ( Monad m
       , MonadThrow m
       )
    => Conduit ByteString m (RDBDatabaseSelector, KVPair)
streamingParser =
  conduitParser go =$=
    CL.map snd     =$=
    --CL.map (\e -> traceShow e e) =$=
    CL.concatMapAccum processStream (RDBDatabaseSelector 0)
  where
    go = EMN <$> rdbMagicNumberP
     <|> EVN <$> rdbVersionNumberP
     <|> EVS <$> rdbDatabaseSelectorP
     <|> EVK <$> kvPairP
    processStream (EVS newSel) _ = (newSel, mempty)
    processStream (EVK pr) sel   = (sel, [(sel, pr)])
    processStream _ sel          = (sel, mempty)


-------------------------------------------------------------------------------
test :: IO ()
test = runResourceT (sourceFile "tmp/dump.rdb" =$= streamingParser $$ CL.mapM_ (liftIO . print))


-------------------------------------------------------------------------------
data EventStream = EMN !RDBMagicNumber
                 | EVN !RDBVersionNumber
                 | EVS !RDBDatabaseSelector
                 | EVK !KVPair deriving (Show)




-------------------------------------------------------------------------------
--TODO: expose a generic streaming interface using the streaming package?
data RDBMagicNumber = RDBMagicNumber
                    -- ^ Token that starts every RDB file
                     deriving (Show, Eq)


-------------------------------------------------------------------------------
data RDBVersionNumber = RDBVersionNumber {
      rdbVersionNumber :: Int
    } deriving (Show, Eq, Ord)


-------------------------------------------------------------------------------
data RDBDatabaseSelector = RDBDatabaseSelector {
      rdbDatabaseSelector :: Int
    } deriving (Show, Eq)



-------------------------------------------------------------------------------
data KVPair = KVPair {
      kvExpiry :: !(Maybe POSIXTime)
    , kvKey    :: !RedisKey
    , kvValue  :: !RedisValue
    } deriving (Show, Eq)


-------------------------------------------------------------------------------
--TODO: namespace
data EOF = EOF deriving (Show, Eq)

-- 0 = “String Encoding”
-- 1 = “List Encoding”
-- 2 = “Set Encoding”
-- 3 = “Sorted Set Encoding”
-- 4 = “Hash Encoding”
-- 9 = “Zipmap Encoding”
-- 10 = “Ziplist Encoding”
-- 11 = “Intset Encoding”
-- 12 = “Sorted Set in Ziplist Encoding”
-- 13 = “Hashmap in Ziplist Encoding” (Introduced in rdb version 4)

data RedisValueType = StringEnc
                    | ListEnc
                    | SetEnc
                    | ZSetEnc
                    | HashEnc
                    | ZipmapEnc
                    | ZiplistEnc
                    | IntsetEnc
                    | ZSetInZiplistEnc
                    | HashmapInZiplistEnc
                    deriving (Show, Eq)


newtype RedisKey = RedisKey {
      redisKey :: ByteString
    } deriving (Show, Eq, Ord)


data RedisString = RString !ByteString
                 | RInt !Int
                 deriving (Show, Eq)


data RedisValue = RVString !RedisString
                | RVList !RedisList
                | RVSet !RedisSet
                deriving (Show, Eq)


newtype RedisList = RedisList {
      redisList :: [ByteString]
    } deriving (Show, Eq)


newtype RedisSet = RedisSet {
      redisSet :: Set ByteString
    } deriving (Show, Eq)


data RCRC64 = RCRC64 {
      crc64 :: Word64
    } deriving (Show, Eq)

-------------------------------------------------------------------------------
-- Parsers
-------------------------------------------------------------------------------

--TODO: use parser tagging

rdbMagicNumberP :: Parser RDBMagicNumber
rdbMagicNumberP = RDBMagicNumber <$ string "REDIS"


-------------------------------------------------------------------------------
rdbVersionNumberP :: Parser RDBVersionNumber
rdbVersionNumberP = fmap RDBVersionNumber . int =<< A.take 4



-------------------------------------------------------------------------------
rdbDatabaseSelectorP :: Parser RDBDatabaseSelector
rdbDatabaseSelectorP = do
  _ <- trace "trying fe" $ word8 0xfe
  len <- trace "got fe" lengthEncoding
  case traceShow ("len rdb sel" :: String, len) len of
    Length l -> trace "successfully got rdb sel!" $ return (RDBDatabaseSelector l)
    x -> traceShow ("not length" :: String, x) $ fail "Expected length encoding to specify database selector"


-------------------------------------------------------------------------------
integerAsStringP :: Parser Int
integerAsStringP = do
  enc <- lengthEncoding
  case enc of
    IntegerAsString sz -> integerAsString sz
    _ -> fail "Expected integer as string"


-------------------------------------------------------------------------------
integerAsString :: IntSize -> Parser Int
integerAsString Int8Size = fromIntegral <$> anyWord8
integerAsString Int16Size = fromIntegral <$> takeWord16
integerAsString Int32Size = fromIntegral <$> takeWord32


-------------------------------------------------------------------------------
kvPairP :: Parser KVPair
kvPairP = do
  --ts <- trace "trying kvPairP" $ optional timestampP
  --_ <- anyWord8 -- maybe timestampP has to consume the byte anyways
  let ts = Nothing
  vt <- trace "skip ts" redisValueTypeP
  --TODO: i think rsk may just be straight length encoded
  rsk <- traceShow ("value type" :: String, vt) redisStringP
  k <- case traceShow ("rsk" :: String, rsk) rsk of
         RString s -> return (RedisKey s)
         _         -> fail "Expected key to be a string but was an integer as string"
  v <- traceShow ("k" :: String, k) $ parseVal vt
  return (traceShow ("v" :: String, v) (KVPair ts k v))


-------------------------------------------------------------------------------
parseVal :: RedisValueType -> Parser RedisValue
parseVal StringEnc = RVString <$> redisStringP
parseVal ListEnc   = RVList <$> redisListP
parseVal SetEnc    = RVSet <$> redisSetP
parseVal _         = undefined


-------------------------------------------------------------------------------
redisListP :: Parser RedisList
redisListP = do
  len <- integerAsStringP
  traceShow ("len" :: String, len) ( RedisList <$> replicateM len redisStringOnlyP)


-------------------------------------------------------------------------------
redisSetP :: Parser RedisSet
redisSetP = RedisSet . S.fromList . redisList <$> redisListP


-------------------------------------------------------------------------------
timestampP :: Parser POSIXTime
timestampP = (word8 0xfd *> parseSecs) <|>
             (word8 0xfc *> parseMillis)
  where
    parseSecs = fmap fromIntegral . int =<< A.take 8
    parseMillis = fmap fromMillis . int =<< A.take 8
    fromMillis millis = realToFrac (millis % 1000) --TODO: break this off and qc


-------------------------------------------------------------------------------
eofP :: Parser EOF
eofP = EOF <$ word8 0xff


-------------------------------------------------------------------------------
checksumP :: Parser RCRC64
checksumP = RCRC64 . fromIntegral <$> takeWord64


-------------------------------------------------------------------------------
data LengthEncoding = Length !Int
                    | IntegerAsString !IntSize
                    | CompressedString
                    deriving (Show)


-------------------------------------------------------------------------------
data IntSize = Int8Size
             | Int16Size
             | Int32Size
             deriving (Show)

-------------------------------------------------------------------------------
--TODO: case on version, rdb 6 is big endian for 4 bytes
lengthEncoding :: Parser LengthEncoding
lengthEncoding = do
  firstByte <- anyWord8
  let twoMSB = (testBit firstByte 7, testBit firstByte 6)
  case traceShow ("firstByte" :: String, firstByte, "twoMSB" :: String, twoMSB) twoMSB of
    (False, False) -> return (Length (fromIntegral (clear2MSB firstByte))) -- next 6 are length
    (False, True) -> do -- read another byte and combine all 14 bits for length
      nextByte :: Word16 <- fromIntegral <$> anyWord8
      let msb6Bits = fromIntegral (clear2MSB firstByte)
      return (Length (fromIntegral ((msb6Bits `shiftL` 8) + nextByte)))
    (True, False) -> do -- discard next 6 bits, read 4 bytes for the length big endian
      Length . fromIntegral <$> takeWord32
    (True, True) -> case traceShow ("clear2MSB firstByte" :: String, firstByte, "->" :: String, clear2MSB firstByte) clear2MSB firstByte of
                      0 -> return (IntegerAsString Int8Size)
                      1 -> return (IntegerAsString Int16Size)
                      2 -> return (IntegerAsString Int32Size)
                      -- doc says 4, redis-rdb-tools says 3. wtf
                      3 -> return CompressedString
                      e -> fail ("Unknown length encoding special type " <> show e)
  where
    clear2MSB :: Word8 -> Word8
    clear2MSB n = n `clearBit` 7 `clearBit` 6


-------------------------------------------------------------------------------
takeWord16 :: Parser Word16
takeWord16 = do
  twoW8s <- fmap fromIntegral . BS.unpack <$> A.take 2
  let w32s = zipWith (\n shifts -> n `shiftL` (8 * shifts)) twoW8s [1,0]
  return (foldl' (+) 0 w32s)


-------------------------------------------------------------------------------
takeWord32 :: Parser Word32
takeWord32 = do
  fourW8s <- fmap fromIntegral . BS.unpack <$> A.take 4
  let w32s = zipWith (\n shifts -> n `shiftL` (8 * shifts)) fourW8s [3,2,1,0]
  return (foldl' (+) 0 w32s)


-------------------------------------------------------------------------------
takeWord64 :: Parser Word64
takeWord64 = do
  fourW8s <- fmap fromIntegral . BS.unpack <$> A.take 4
  let w64s = zipWith (\n shifts -> n `shiftL` (8 * shifts)) fourW8s [7,6,5,4,3,2,1,0]
  return (foldl' (+) 0 w64s)


-------------------------------------------------------------------------------
-- this is buggy?
redisStringP :: Parser RedisString
redisStringP = go . (\le -> traceShow ("redisStringP length encoding" :: String, le) le) =<< lengthEncoding
  where
    go (Length n) = traceShow ("taking" :: String, n) $ do --(RString <$> A.take n)
      s <- A.take n
      --TODO: why does this fail?
      return (traceShow ("took" :: String, s) (RString s))
    go CompressedString = RString <$> compressedString
    go (IntegerAsString sz) = RInt <$> integerAsString sz


-------------------------------------------------------------------------------
redisStringOnlyP :: Parser ByteString
redisStringOnlyP = do
  rs <- redisStringP
  case rs of
    RString s -> return s
    RInt _ -> fail "Expected RString but got RInt"


-------------------------------------------------------------------------------
-- | LZF-compressed bytestring
compressedString :: Parser ByteString
compressedString = do
  mcompressedLen <- lengthEncoding
  muncompressedLen <- lengthEncoding
  case (mcompressedLen, muncompressedLen) of
    (Length compressedLen, Length uncompressedLen) -> do
      compressed <- LZFCompressed <$> A.take compressedLen
      let res = decompress (KnownUncompressedSize uncompressedLen) compressed
      either (fail . show) return res
    _ -> fail "Expected compressed/uncompressed length to be integers"


-------------------------------------------------------------------------------
redisValueTypeP :: Parser RedisValueType
redisValueTypeP = do
  n <- anyWord8
  case traceShow ("n" :: String, n) n of
    0  -> return StringEnc
    1  -> return ListEnc
    2  -> return SetEnc
    3  -> return ZSetEnc
    4  -> return HashEnc
    9  -> return ZipmapEnc
    10 -> return ZiplistEnc
    11 -> return IntsetEnc
    12 -> return ZSetInZiplistEnc
    13 -> return HashmapInZiplistEnc
    _  -> fail ("Unknown redis value type " <> show n)



-------------------------------------------------------------------------------
int :: Monad m => ByteString -> m Int
int bs = case BS8.readInt bs of
           Just (i, "") -> return i
           Just (_, extra) -> fail ("extraneous data parsing int: " <> show extra)
           Nothing -> fail "Could not parse int"
