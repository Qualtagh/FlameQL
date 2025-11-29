import { collection, Collection, collectionGroup } from '../src/api/collection';
import { Field } from '../src/api/field';
import { Literal, LiteralType } from '../src/api/literal';

test('collection parsing (table-driven)', () => {
  const t = (actual: Collection, expected: Collection) => expect(actual).toEqual(expected);
  t(collection('jobs'), new Collection({ group: false, path: [new Literal('jobs', LiteralType.String)] }));
  t(collection('{jobId}'), new Collection({ group: false, path: [new Field('jobId', [])] }));
  t(collection('{j.id}'), new Collection({ group: false, path: [new Field('j', ['id'])] }));
  t(
    collection('jobs/{j.id}/shifts'),
    new Collection({
      group: true,
      path: [new Literal('jobs', LiteralType.String), new Field('j', ['id']), new Literal('shifts', LiteralType.String)],
    })
  );
  t(collectionGroup('shifts'), new Collection({ group: true, path: [new Literal('shifts', LiteralType.String)] }));
});
