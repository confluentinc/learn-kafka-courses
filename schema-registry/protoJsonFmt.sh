PROTO_JSON=$(awk '{gsub(/\n/,"\\\n"; gsub(/"/, "\\\"");print}' $1) \
  && SCHEMA="{\"schemaType\":\"PROTOBUF\",\"schema\":\"${PROTO_JSON}\"\n}" \
  && echo ${SCHEMA}
