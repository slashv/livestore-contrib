import { Effect, Schema } from '@livestore/utils/effect'

import { defineAction, defineApplication, defineInspector } from '../../application/definition.ts'
import { scenarioApplication } from '../../scenario.ts'
import { todoEvents, todoSchema, todosTable } from './todo-schema.ts'

export { todoEvents, todoSchema, todosTable } from './todo-schema.ts'

const CreateTodoInput = Schema.Struct({ id: Schema.String, text: Schema.String })
const CreateTodosInput = Schema.Struct({
  idPrefix: Schema.String,
  start: Schema.Int,
  count: Schema.Int,
  textBytes: Schema.Int,
})
const EditTodoInput = Schema.Struct({ id: Schema.String, text: Schema.String })
const SetTodoCompletedInput = Schema.Struct({ id: Schema.String, completed: Schema.Boolean })
const DeleteTodoInput = Schema.Struct({ id: Schema.String })
const TodoRows = Schema.Array(Schema.Struct({ id: Schema.String, text: Schema.String, completed: Schema.Boolean }))

export const todoApplication = defineApplication({
  id: 'scenario-todo-app',
  scenarioName: 'todo',
  schema: todoSchema,
  actions: {
    createTodo: defineAction<typeof todoSchema, typeof CreateTodoInput>({
      input: CreateTodoInput,
      run: ({ store, input }) => Effect.sync(() => store.commit(todoEvents.created(input))),
    }),
    /**
     * Creates `count` todos in one commit, so stress Scenarios can build large histories and pending tails without one
     * harness round trip per event. IDs are `<idPrefix>-<n>` with `n` zero-padded from `start`.
     */
    createTodos: defineAction<typeof todoSchema, typeof CreateTodosInput>({
      input: CreateTodosInput,
      run: ({ store, input }) =>
        Effect.sync(() => {
          const events = Array.from({ length: input.count }, (_, offset) => {
            const n = input.start + offset
            const label = `Todo ${input.idPrefix} ${n} `
            return todoEvents.created({
              id: `${input.idPrefix}-${String(n).padStart(6, '0')}`,
              text: label.padEnd(Math.max(label.length, input.textBytes), '.'),
            })
          })
          store.commit(...events)
        }),
    }),
    editTodo: defineAction<typeof todoSchema, typeof EditTodoInput>({
      input: EditTodoInput,
      run: ({ store, input }) => Effect.sync(() => store.commit(todoEvents.textChanged(input))),
    }),
    setTodoCompleted: defineAction<typeof todoSchema, typeof SetTodoCompletedInput>({
      input: SetTodoCompletedInput,
      run: ({ store, input }) => Effect.sync(() => store.commit(todoEvents.completionChanged(input))),
    }),
    deleteTodo: defineAction<typeof todoSchema, typeof DeleteTodoInput>({
      input: DeleteTodoInput,
      run: ({ store, input }) => Effect.sync(() => store.commit(todoEvents.deleted(input))),
    }),
  },
  inspectors: {
    todos: defineInspector<typeof todoSchema, typeof TodoRows>({
      output: TodoRows,
      read: ({ store }) => store.query(todosTable).toSorted((left, right) => left.id.localeCompare(right.id)),
    }),
  },
})

/** Typed Scenario-as-code action vocabulary for the Todo Application. */
export const todo = scenarioApplication(todoApplication)
