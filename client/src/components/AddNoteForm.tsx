import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Form,
  FormControl,
  FormField,
  FormItem,
  FormLabel,
  FormMessage,
} from "@/components/ui/form";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { Button } from "@/components/ui/button";
import { useAddNote } from "@/hooks/useBuildings";
import { useToast } from "@/hooks/use-toast";

const formSchema = z.object({
  noteType: z.string().min(1, "Note type is required"),
  content: z.string().min(5, "Note must be at least 5 characters"),
});

type FormValues = z.infer<typeof formSchema>;

const NOTE_TYPES = [
  { value: "access-code", label: "Access Code" },
  { value: "riser-location", label: "Riser Location" },
  { value: "parking", label: "Parking" },
  { value: "entrance", label: "Entrance" },
  { value: "gate-code", label: "Gate Code" },
  { value: "lockbox", label: "Lockbox" },
  { value: "general", label: "General" },
];

interface AddNoteFormProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  buildingId: number;
}

export default function AddNoteForm({
  open,
  onOpenChange,
  buildingId,
}: AddNoteFormProps) {
  const { toast } = useToast();
  const addNote = useAddNote();

  const form = useForm<FormValues>({
    resolver: zodResolver(formSchema),
    defaultValues: {
      noteType: "",
      content: "",
    },
  });

  async function onSubmit(values: FormValues) {
    await addNote.mutateAsync({
      buildingId,
      noteType: values.noteType,
      content: values.content,
      source: "community",
      createdAt: new Date().toISOString(),
    });
    toast({ title: "Note added" });
    form.reset();
    onOpenChange(false);
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-md">
        <DialogHeader>
          <DialogTitle>Add Field Note</DialogTitle>
        </DialogHeader>

        <Form {...form}>
          <form onSubmit={form.handleSubmit(onSubmit)} className="space-y-4">
            {/* Note Type */}
            <FormField
              control={form.control}
              name="noteType"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Note Type</FormLabel>
                  <Select onValueChange={field.onChange} value={field.value}>
                    <FormControl>
                      <SelectTrigger data-testid="select-note-type">
                        <SelectValue placeholder="Select type" />
                      </SelectTrigger>
                    </FormControl>
                    <SelectContent>
                      {NOTE_TYPES.map((t) => (
                        <SelectItem key={t.value} value={t.value}>
                          {t.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  <FormMessage />
                </FormItem>
              )}
            />

            {/* Content */}
            <FormField
              control={form.control}
              name="content"
              render={({ field }) => (
                <FormItem>
                  <FormLabel>Note Content</FormLabel>
                  <FormControl>
                    <Textarea
                      {...field}
                      placeholder="e.g. Riser room is on basement level, door code 4321#"
                      className="resize-none min-h-[100px]"
                      data-testid="textarea-note-content"
                    />
                  </FormControl>
                  <FormMessage />
                </FormItem>
              )}
            />

            <div className="flex gap-2 justify-end pt-1">
              <Button
                type="button"
                variant="ghost"
                onClick={() => onOpenChange(false)}
                data-testid="button-cancel-note"
              >
                Cancel
              </Button>
              <Button
                type="submit"
                disabled={addNote.isPending}
                data-testid="button-submit-note"
              >
                {addNote.isPending ? "Adding..." : "Add Note"}
              </Button>
            </div>
          </form>
        </Form>
      </DialogContent>
    </Dialog>
  );
}
