
  #Remove file, the script will create/append to the file.
if test -f "$2"; then
    rm $2
fi

#Decode metrics bson file into json.
bsondump --quiet $1 | jq 'select( .type."$numberInt" == "1" ) | .data."$binary".base64' | jq -s '.' > EncodedArray.json

#Get number of chunks.
EncodedArrayLength=$( cat EncodedArray.json | jq 'length' )
echo "$EncodedArrayLength Encoded Chunks Found"

#The Base64 decoding and bsondump MUST be done separately for each chunk.
for (( index=0; index<$EncodedArrayLength; index++ ))
do
    echo -ne "Decoding Chunk $index\033[0K\r"
    cat EncodedArray.json | jq --argjson i $index '.[$i]' | ruby -rzlib -rbase64 -e 'd = STDIN.read; print Zlib::Inflate.new.inflate(Base64.decode64(d)[4..-1])' > "DecodedArray.bson"

    #Append resulting chunk json data to the end of the file
    bsondump --quiet DecodedArray.bson | jq -s '.' >> $2
done

#Clean up temp files used.
rm DecodedArray.bson
rm EncodedArray.json
echo "All Chunks Decoded"