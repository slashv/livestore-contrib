import { Events, makeSchema, State } from '@livestore/common/schema'
import { Schema } from '@livestore/utils/effect'

/**
 * LiveStore schema of the Todo Application, without the scenario action vocabulary, so non-runner code such as a
 * Cloudflare Worker (the snapshot boot checkpointer) can import it. Its snapshot key must match the clients' schema.
 */
export const todoEvents = {
  created: Events.synced({
    name: 'v1.TodoCreated',
    schema: Schema.Struct({ id: Schema.String, text: Schema.String }),
  }),
  textChanged: Events.synced({
    name: 'v1.TodoTextChanged',
    schema: Schema.Struct({ id: Schema.String, text: Schema.String }),
  }),
  completionChanged: Events.synced({
    name: 'v1.TodoCompletionChanged',
    schema: Schema.Struct({ id: Schema.String, completed: Schema.Boolean }),
  }),
  deleted: Events.synced({
    name: 'v1.TodoDeleted',
    schema: Schema.Struct({ id: Schema.String }),
  }),
}

export const todosTable = State.SQLite.table({
  name: 'todos',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    text: State.SQLite.text({ nullable: false }),
    completed: State.SQLite.boolean({ default: false, nullable: false }),
  },
})

const materializers = State.SQLite.materializers(todoEvents, {
  'v1.TodoCreated': ({ id, text }) => todosTable.insert({ id, text, completed: false }),
  'v1.TodoTextChanged': ({ id, text }) => todosTable.update({ text }).where({ id }),
  'v1.TodoCompletionChanged': ({ id, completed }) => todosTable.update({ completed }).where({ id }),
  'v1.TodoDeleted': ({ id }) => todosTable.delete().where({ id }),
})

export const todoSchema = makeSchema({
  events: todoEvents,
  state: State.SQLite.makeState({
    tables: { todos: todosTable },
    materializers,
  }),
})
