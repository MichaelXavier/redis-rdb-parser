module Main
    ( main
    ) where


-------------------------------------------------------------------------------
import           Development.Shake
import           Development.Shake.FilePath
-------------------------------------------------------------------------------


main :: IO ()
main = shakeArgs shakeOptions $ do
  want ["test/data/example.rdb"]

  "tmp" %> \out -> cmd "mkdir" ["-p", out]

  "test/data/example.rdb" %> \out -> do
    need [ "test/data/example.conf"
         , "generate_dump.sh"
         , "tmp"
         ]
    cmd "./generate_dump.sh"
