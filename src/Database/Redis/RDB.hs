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
    , RedisZiplist(..)
    , RedisZipmap(..)
    , RedisHMZiplist(..)
    , RedisZSZiplist(..)
    , RedisZLEntry(..)
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
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Resource
import           Control.Monad.Trans.State.Strict
import           Data.Attoparsec.Binary
import           Data.Attoparsec.ByteString       as A
import           Data.Bits
import           Data.ByteString                  (ByteString)
import           Data.Int
import           Data.Ix
-- import qualified Data.ByteString              as BS
import qualified Data.ByteString.Char8            as BS8
import           Data.Conduit
import           Data.Conduit.Attoparsec
import           Data.Conduit.Binary
import qualified Data.Conduit.List                as CL
import           Data.Monoid
import           Data.Ratio
import           Data.Set                         (Set)
import qualified Data.Set                         as S
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
                | RVIntSet !RedisIntSet
                | RVZiplist !RedisZiplist
                | RVZipmap !RedisZipmap
                | RVHashmapInZiplist !RedisHMZiplist
                | RVZSetInZiplistEnc !RedisZSZiplist
                deriving (Show, Eq)


newtype RedisList = RedisList {
      redisList :: [ByteString]
    } deriving (Show, Eq)


newtype RedisSet = RedisSet {
      redisSet :: Set ByteString
    } deriving (Show, Eq)


data RedisZiplist = RedisZiplist {
      zlBytes   :: !Word32
    , zlTail    :: !Word32
    , zlLen     :: !Word16
    , zlEntries :: ![RedisZLEntry]
    } deriving (Show, Eq)


data RedisZipmap = RedisZipmap {
      zmLen   :: !(Maybe Word8)
    , zmItems :: ![ByteString]
    } deriving (Show, Eq)


newtype RedisHMZiplist = RedisHMZiplist {
      redisHMZiplist :: RedisZiplist
    } deriving (Show, Eq)


newtype RedisZSZiplist = RedisZSZiplist {
      redisZSZiplist :: RedisZiplist
    } deriving (Show, Eq)


--TODO: we should determine if the goal here is to coerce to
--reasonable haskell types or to keep it very close to internal
--representation for the purpose of counting bits.
data RedisZLEntry = RedisZLEntry {
      zlePrevEntryLength :: !(Either Word8 Word32)
    , zleValue           :: !RedisString
    } deriving (Show, Eq)


--TODO: is the intset unsigned or signed?
data RedisIntSet = RedisIntSet16 !(Set Word16)
                 | RedisIntSet32 !(Set Word32)
                 | RedisIntSet64 !(Set Word64)
                 deriving (Show, Eq)


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
-- | Selects only the int-oriented variants of length encoding
intLengthEncodingP :: Parser Int
intLengthEncodingP = do
  enc <- lengthEncoding
  case enc of
    Length n           -> return n
    IntegerAsString sz -> integerAsString sz
    _                  -> fail "Expected integer as string"


-------------------------------------------------------------------------------
integerAsString :: IntSize -> Parser Int
integerAsString Int8Size = fromIntegral <$> anyWord8
integerAsString Int16Size = fromIntegral <$> anyWord16be
integerAsString Int32Size = fromIntegral <$> anyWord32be


-------------------------------------------------------------------------------
kvPairP :: Parser KVPair
kvPairP = do
  ts <- trace "trying kvPairP" $ optional timestampP
  --_ <- anyWord8 -- maybe timestampP has to consume the byte anyways
  -- let ts = Nothing
  vt <- redisValueTypeP
  --TODO: i think rsk may just be straight length encoded
  rsk <- traceShow ("value type" :: String, vt) redisStringP
  k <- case traceShow ("rsk" :: String, rsk) rsk of
         RString s -> return (RedisKey s)
         _         -> fail "Expected key to be a string but was an integer as string"
  v <- traceShow ("k" :: String, k) $ parseVal vt
  return (traceShow ("v" :: String, v) (KVPair ts k v))


-------------------------------------------------------------------------------
parseVal :: RedisValueType -> Parser RedisValue
parseVal StringEnc           = RVString <$> redisStringP
parseVal ListEnc             = RVList <$> redisListP
parseVal SetEnc              = RVSet <$> redisSetP
parseVal ZSetEnc             = error "TODO: ZSetEnc" --FIXME: docs just TODO this...
parseVal HashEnc             = RVHash <$> rvHash
parseVal ZipmapEnc           = RVZipmap <$> redisZipmapP
parseVal ZiplistEnc          = RVZiplist <$> redisZiplistP
parseVal IntsetEnc           = RVIntSet <$> redisIntSetP
parseVal ZSetInZiplistEnc    = RVZSetInZiplistEnc <$> redisZSetInZiplistP
parseVal HashmapInZiplistEnc = RVHashmapInZiplist <$> redisHashmapInZiplistP


-------------------------------------------------------------------------------
redisListP :: Parser RedisList
redisListP = do
  len <- intLengthEncodingP
  traceShow ("llen" :: String, len) ( RedisList <$> replicateM len redisStringOnlyP)


-------------------------------------------------------------------------------
redisSetP :: Parser RedisSet
redisSetP = RedisSet . S.fromList . redisList <$> redisListP


-------------------------------------------------------------------------------
redisIntSetP :: Parser RedisIntSet
redisIntSetP = either fail return . parseOnly parseEnvelope =<< redisStringOnlyP
  where
    parseEnvelope = do
      enc <- anyWord32le
      case traceShow ("enc" :: String, enc) enc of
        2 -> RedisIntSet16 <$> go anyWord16le
        4 -> RedisIntSet32 <$> go anyWord32le
        8 -> RedisIntSet64 <$> go anyWord64le
        x -> fail ("Invalid intset encoding " <> show x)
    go parser = do
      len <- anyWord32le
      traceShow ("len" :: String, len) (S.fromList <$> replicateM (fromIntegral len) parser)


-------------------------------------------------------------------------------
-- I think technically we aren't supposed to take integer keys?
redisHashmapInZiplistP :: Parser RedisHMZiplist
redisHashmapInZiplistP = RedisHMZiplist <$> redisZiplistP


-------------------------------------------------------------------------------
redisZSetInZiplistP :: Parser RedisZSZiplist
redisZSetInZiplistP = RedisZSZiplist <$> redisZiplistP


-------------------------------------------------------------------------------
redisZiplistP :: Parser RedisZiplist
redisZiplistP = either fail return . parseOnly parseEnvelope =<< redisStringOnlyP
  where
    parseEnvelope = do
      zlb <- anyWord32le
      zlt <- traceShow ("zlb" :: String, zlb) anyWord32le
      zll <- traceShow ("zlt" :: String, zlt) anyWord16le
      entries <- traceShow ("zll" :: String, zll) $ replicateM (fromIntegral zll) redisZLEntryP
      _zlend <- traceShow ("entries" :: String, entries) $ word8 255
      return (RedisZiplist zlb zlt zll entries)


-------------------------------------------------------------------------------
--TODO: find an old database that still uses zipmaps
redisZipmapP :: Parser RedisZipmap
redisZipmapP = do
  len <- anyWord8
  let zml = if len >= 254
               then Nothing
               else Just len
  let expectFree = False -- first one
  items <- evalStateT (many itemP) expectFree
  _ <- word8 255
  return (RedisZipmap zml items)
  where
    itemP = do
      len <- lift itemLen
      expectFree <- get
      free <- if expectFree
                 then lift freeP
                 else put True >> return 0
      lift $ do
        s <- A.take len
        _ <- A.take (fromIntegral free)
        return s
    freeP = anyWord8
    itemLen = do
      firstByte <- anyWord8
      case firstByte of
        253 -> fromIntegral <$> anyWord32be
        n | inRange (0, 252) n -> return (fromIntegral n)
        _ -> fail "item length must be between 0 and 253"


-------------------------------------------------------------------------------
redisZLEntryP :: Parser RedisZLEntry
redisZLEntryP = do
  lenFirst <- anyWord8
  prevLen <- if lenFirst <= 253
               then return (Left lenFirst)
               else Right <$> anyWord32be
  firstByte <- testBit8 <$> peekWord8'
  str <- case traceShow firstByte firstByte of
    (O,O,_,_,_,_,_,_) -> fmap RString . A.take . fromIntegral =<< anyWord8
    (O,I,_,_,_,_,_,_) -> do -- take 16 bits, ignore these first two, that's strlen
      len14 <- (`clearBit` 14) <$> anyWord16be -- only need to set this one I to off
      RString <$> A.take (fromIntegral len14)
    (I,O,_,_,_,_,_,_) -> do
      _ <- anyWord8 -- trash this byte we're focusing on, take next 4
      fmap RString . A.take . fromIntegral =<< anyWord32be
    (I,I,O,O,_,_,_,_) -> do -- next 2 bytes are 16 bit signed
      _ <- anyWord8
      RInt . fromIntegral . w16i <$> anyWord16be
    (I,I,O,I,_,_,_,_) -> do -- next 4 bytes are 32 bit signed
      _ <- anyWord8
      RInt . fromIntegral . w32i <$> anyWord32be
    (I,I,I,O,_,_,_,_) -> do -- next 8 bytes are 64 bit signed
      _ <- anyWord8
      RInt . fromIntegral . w64i <$> anyWord64be
    (I,I,I,I,O,O,O,O) -> do -- next 3 bytes as 24 bit signed integer
      _ <- anyWord8
      RInt . fromIntegral . i24i <$> anyInt24BE
    (I,I,I,I,I,I,I,O) -> do -- next byte as 8 bit signed
      _ <- anyWord8
      RInt . fromIntegral . w8i . (\x -> traceShow ("w8i" :: String, x) x) <$> anyWord8
    (I,I,I,I,_,_,_,_) -> -- remaining is unsigned int 4 from 0000 to 1101 - 1
      -- take off first 4 bits with 0xf mask
      RInt . fromIntegral . pred . (0xf .&.) <$> anyWord8
  return (RedisZLEntry prevLen str)


-------------------------------------------------------------------------------
newtype Int24BE = Int24BE (Word8, Word8, Word8)


anyInt24BE :: Parser Int24BE
anyInt24BE = Int24BE <$> ((,,) <$> anyWord8 <*> anyWord8 <*> anyWord8)


--uh how do we know we're gettign this signage right?
i24i :: Int24BE -> Int32
i24i (Int24BE (w1, w2, w3)) = foldr unstep 0 [w1, w2, w3]
  where
    unstep b a = a `shiftL` 8 .|. fromIntegral b


-------------------------------------------------------------------------------
w8i :: Word8 -> Int8
w8i = fromIntegral


-------------------------------------------------------------------------------
w16i :: Word16 -> Int16
w16i = fromIntegral


-------------------------------------------------------------------------------
w32i :: Word32 -> Int32
w32i = fromIntegral


-------------------------------------------------------------------------------
w64i :: Word64 -> Int64
w64i = fromIntegral


-------------------------------------------------------------------------------
-- | Cute way of modeling readable masks
data B = O | I deriving (Show)


testBit8 :: Word8 -> (B,B,B,B,B,B,B,B)
testBit8 w = (t 7, t 6, t 5, t 4, t 3, t 2, t 1, t 0)
  where
    t n = if testBit w n then I else O

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
checksumP = RCRC64 . fromIntegral <$> anyWord64be


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
  --TODO: use 8 bit fields trick for better readability
  let twoMSB =  (testBit firstByte 7, testBit firstByte 6)
  case twoMSB of
    (False, False) -> return (Length (fromIntegral (clear2MSB firstByte))) -- next 6 are length
    (False, True) -> do -- read another byte and combine all 14 bits for length
      nextByte :: Word16 <- fromIntegral <$> anyWord8
      let msb6Bits = fromIntegral (clear2MSB firstByte)
      return (Length (fromIntegral ((msb6Bits `shiftL` 8) + nextByte)))
    (True, False) -> do -- discard next 6 bits, read 4 bytes for the length big endian
      Length . fromIntegral <$> anyWord32be
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
    0x0  -> return StringEnc
    0x1  -> return ListEnc
    0x2  -> return SetEnc
    0x3  -> return ZSetEnc
    0x4  -> return HashEnc
    0x9  -> return ZipmapEnc
    0xa -> return ZiplistEnc
    0xb -> return IntsetEnc
    0xc -> return ZSetInZiplistEnc
    0xd -> return HashmapInZiplistEnc
    _  -> fail ("Unknown redis value type " <> show n)



-------------------------------------------------------------------------------
int :: Monad m => ByteString -> m Int
int bs = case BS8.readInt bs of
           Just (i, "") -> return i
           Just (_, extra) -> fail ("extraneous data parsing int: " <> show extra)
           Nothing -> fail "Could not parse int"
