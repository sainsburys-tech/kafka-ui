import React from 'react';
import Editor from 'components/common/Editor/Editor';
import { SchemaType } from 'generated-sources';
import { parse, stringify } from 'lossless-json';

import * as S from './EditorViewer.styled';

export interface EditorViewerProps {
  data: string;
  schemaType?: string;
  maxLines?: number;
}
const getSchemaValue = (data: string, schemaType?: string) => {
  if (schemaType === SchemaType.JSON || schemaType === SchemaType.AVRO) {
    return stringify(parse(data), undefined, '\t');
  }
  return data;
};
const EditorViewer: React.FC<EditorViewerProps> = ({
  data,
  schemaType,
  maxLines,
}) => {
  try {
    return (
      <S.Wrapper>
      {data.length > 1000000 ? (
        <Editor
          isFixedHeight
          schemaType={schemaType}
          name="schema"
          value="The message is too large. We can’t display this message due to performance issues. However, you can copy to clipboard or save as a file."
          setOptions={{
            showLineNumbers: false,
            maxLines: 2,
            showGutter: false,
          }}
          readOnly
        />
        ) : (
        <Editor
          isFixedHeight
          schemaType={schemaType}
          name="schema"
          value={getSchemaValue(data, schemaType)}
          setOptions={{
            showLineNumbers: false,
            maxLines,
            showGutter: false,
          }}
          readOnly
        />
        )}
      </S.Wrapper>
    );
  } catch (e) {
    return (
      <S.Wrapper>
        <p>{data}</p>
      </S.Wrapper>
    );
  }
};

export default EditorViewer;
