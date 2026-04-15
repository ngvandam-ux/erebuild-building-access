import { useState } from "react";
import {
  ArrowLeft,
  Phone,
  Mail,
  CheckCircle2,
  Circle,
  Plus,
  ThumbsUp,
  Building2,
  MapPin,
  Users,
  Calendar,
  Ruler,
  User,
  Pencil,
  Check,
  X,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Separator } from "@/components/ui/separator";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { useBuildingDetail, useVerifyContact, useUpvoteNote, useUpdateContact, useUpdateNote } from "@/hooks/useBuildings";
import type { Building, Contact, BuildingNote } from "@/hooks/useBuildings";
import { useToast } from "@/hooks/use-toast";
import AddContactForm from "./AddContactForm";
import AddNoteForm from "./AddNoteForm";

// ─── Building type display config ──────────────────────────────────────────
const TYPE_CONFIG: Record<string, { label: string; color: string }> = {
  apartment:  { label: "Apartment",  color: "bg-[#D4A547]/15 text-[#D4A547]" },
  commercial: { label: "Commercial", color: "bg-[#5B9BD5]/15 text-[#5B9BD5]" },
  office:     { label: "Office",     color: "bg-[#9B8EC4]/15 text-[#9B8EC4]" },
  industrial: { label: "Industrial", color: "bg-[#7C8A96]/15 text-[#7C8A96]" },
  government: { label: "Government", color: "bg-[#4ADE80]/15 text-[#4ADE80]" },
  school:        { label: "School",        color: "bg-[#34D399]/15 text-[#34D399]" },
  hospital:      { label: "Hospital",      color: "bg-[#F87171]/15 text-[#F87171]" },
  retail:        { label: "Retail",        color: "bg-[#C17A2E]/15 text-[#C17A2E]" },
  "mixed-use":   { label: "Mixed-Use",     color: "bg-[#C084FC]/15 text-[#C084FC]" },
  healthcare:    { label: "Healthcare",    color: "bg-[#FB923C]/15 text-[#FB923C]" },
  nursing_home:  { label: "Nursing Home",  color: "bg-[#FDA4AF]/15 text-[#FDA4AF]" },
  boarding_care: { label: "Boarding Care", color: "bg-[#FDBA74]/15 text-[#FDBA74]" },
  childcare:     { label: "Childcare",     color: "bg-[#A78BFA]/15 text-[#A78BFA]" },
};

const ROLE_LABELS: Record<string, string> = {
  "on-site-super":    "On-Site Super",
  "property-manager": "Property Manager",
  "property-owner":   "Property Owner",
  "leasing-office":   "Leasing Office",
  "owner":            "Owner",
  "maintenance":      "Maintenance",
  "other":            "Other",
};

const SOURCE_LABELS: Record<string, string> = {
  hud:                    "HUD",
  "rental-license":       "Rental License",
  "mpls-rental-license":  "Mpls Rental License",
  "google-places":        "Google",
  "skip-trace":           "Skip Trace",
  community:              "Community",
  "hennepin-assessor-owner": "Hennepin Assessor",
  "metrogis-owner":       "MetroGIS Parcels",
  "metrogis-6-counties-owner": "MetroGIS",
  "rental-license-owner": "Rental License",
  "hud-owner":                  "HUD",
  "osm-overpass":                "OpenStreetMap",
  "irs-eo":                      "IRS Nonprofits",
  "mn-hospitals":                "MN Hospitals",
  "mn-nursing-homes":            "MN Nursing Homes",
  "mn-doh-healthcare":           "MN DOH Healthcare",
  "mn-schools":                  "MN Schools",
  "mpls-liquor-license":         "Mpls Liquor License",
  "cms-nursing-home":            "CMS Nursing Home",
  "cms-hospital":                "CMS Hospital",
  "cms-home-health":             "CMS Home Health",
  "cms-dialysis":                "CMS Dialysis",
  "metrogis-ramsey-owner":       "MetroGIS Ramsey",
  "metrogis-dakota-owner":       "MetroGIS Dakota",
  "metrogis-anoka-owner":        "MetroGIS Anoka",
  "metrogis-washington-owner":   "MetroGIS Washington",
  "community-owner":             "Community",
  "mn-childcare":                 "MN Childcare",
  "mn-dhs-childcare":             "MN DHS Childcare",
  "npi-registry":                 "NPI Registry",
};

const NOTE_TYPE_LABELS: Record<string, string> = {
  "access-code":     "Access Code",
  "riser-location":  "Riser Location",
  parking:           "Parking",
  entrance:          "Entrance",
  "gate-code":       "Gate Code",
  lockbox:           "Lockbox",
  general:           "General",
};

// ─── Sub-components ─────────────────────────────────────────────────────────
function InfoPill({ icon: Icon, text }: { icon: React.ElementType; text: string }) {
  return (
    <span className="inline-flex items-center gap-1 text-xs text-muted-foreground">
      <Icon className="w-3 h-3 shrink-0" />
      {text}
    </span>
  );
}

// ─── Inline edit sub-component for contacts ─────────────────────────────────
function EditableContact({
  contact,
  buildingId,
  onDone,
}: {
  contact: Contact;
  buildingId: number;
  onDone: () => void;
}) {
  const [name, setName] = useState(contact.name ?? "");
  const [phone, setPhone] = useState(contact.phone ?? "");
  const [email, setEmail] = useState(contact.email ?? "");
  const [notes, setNotes] = useState(contact.notes ?? "");
  const updateContact = useUpdateContact();
  const { toast } = useToast();

  const handleSave = async () => {
    await updateContact.mutateAsync({
      id: contact.id,
      buildingId,
      updates: {
        name: name || null,
        phone: phone || null,
        email: email || null,
        notes: notes || null,
      },
    });
    toast({ title: "Contact updated" });
    onDone();
  };

  return (
    <div className="space-y-2 pt-2 border-t border-border">
      <div className="grid grid-cols-2 gap-2">
        <Input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="Name"
          className="h-7 text-xs"
          data-testid={`input-edit-contact-name-${contact.id}`}
        />
        <Input
          value={phone}
          onChange={(e) => setPhone(e.target.value)}
          placeholder="Phone"
          className="h-7 text-xs"
          data-testid={`input-edit-contact-phone-${contact.id}`}
        />
      </div>
      <Input
        value={email}
        onChange={(e) => setEmail(e.target.value)}
        placeholder="Email"
        className="h-7 text-xs"
        data-testid={`input-edit-contact-email-${contact.id}`}
      />
      <Textarea
        value={notes}
        onChange={(e) => setNotes(e.target.value)}
        placeholder="Notes"
        className="resize-none text-xs min-h-[48px]"
        data-testid={`textarea-edit-contact-notes-${contact.id}`}
      />
      <div className="flex gap-1 justify-end">
        <Button
          size="sm"
          variant="ghost"
          className="h-6 text-xs px-2"
          onClick={onDone}
          data-testid={`button-cancel-edit-contact-${contact.id}`}
        >
          <X className="w-3 h-3 mr-1" />
          Cancel
        </Button>
        <Button
          size="sm"
          className="h-6 text-xs px-2"
          onClick={handleSave}
          disabled={updateContact.isPending}
          data-testid={`button-save-edit-contact-${contact.id}`}
        >
          <Check className="w-3 h-3 mr-1" />
          {updateContact.isPending ? "Saving..." : "Save"}
        </Button>
      </div>
    </div>
  );
}

// ─── Inline edit sub-component for notes ────────────────────────────────────
function EditableNote({
  note,
  buildingId,
  onDone,
}: {
  note: BuildingNote;
  buildingId: number;
  onDone: () => void;
}) {
  const [content, setContent] = useState(note.content);
  const updateNote = useUpdateNote();
  const { toast } = useToast();

  const handleSave = async () => {
    await updateNote.mutateAsync({
      id: note.id,
      buildingId,
      updates: { content },
    });
    toast({ title: "Note updated" });
    onDone();
  };

  return (
    <div className="space-y-2 pt-2 border-t border-border">
      <Textarea
        value={content}
        onChange={(e) => setContent(e.target.value)}
        className="resize-none text-xs min-h-[60px]"
        data-testid={`textarea-edit-note-content-${note.id}`}
      />
      <div className="flex gap-1 justify-end">
        <Button
          size="sm"
          variant="ghost"
          className="h-6 text-xs px-2"
          onClick={onDone}
          data-testid={`button-cancel-edit-note-${note.id}`}
        >
          <X className="w-3 h-3 mr-1" />
          Cancel
        </Button>
        <Button
          size="sm"
          className="h-6 text-xs px-2"
          onClick={handleSave}
          disabled={updateNote.isPending}
          data-testid={`button-save-edit-note-${note.id}`}
        >
          <Check className="w-3 h-3 mr-1" />
          {updateNote.isPending ? "Saving..." : "Save"}
        </Button>
      </div>
    </div>
  );
}

// ─── Main component ──────────────────────────────────────────────────────────
interface BuildingDetailProps {
  building: Building;
  onBack: () => void;
}

export default function BuildingDetail({ building, onBack }: BuildingDetailProps) {
  const { data, isLoading } = useBuildingDetail(building.id);
  const verifyContact = useVerifyContact();
  const upvoteNote = useUpvoteNote();

  const [addContactOpen, setAddContactOpen] = useState(false);
  const [addNoteOpen, setAddNoteOpen] = useState(false);
  const [editingContactId, setEditingContactId] = useState<number | null>(null);
  const [editingNoteId, setEditingNoteId] = useState<number | null>(null);

  const typeConf = TYPE_CONFIG[building.building_type ?? ""] ?? {
    label: building.building_type ?? "Unknown",
    color: "bg-slate-500/15 text-slate-600 dark:text-slate-400",
  };

  const displayName = building.name ?? building.address;
  const subAddress = building.name ? building.address : null;

  return (
    <div className="flex flex-col h-full">
      {/* ── Header ── */}
      <div className="flex items-start gap-2 px-4 py-3 border-b border-border shrink-0">
        <Button
          size="icon"
          variant="ghost"
          onClick={onBack}
          className="mt-0.5 shrink-0"
          data-testid="button-back-to-list"
        >
          <ArrowLeft className="w-4 h-4" />
        </Button>
        <div className="min-w-0">
          <h2 className="font-semibold text-sm leading-tight truncate" data-testid="text-building-name">
            {displayName}
          </h2>
          {subAddress && (
            <p className="text-xs text-muted-foreground truncate mt-0.5" data-testid="text-building-address">
              {subAddress}
            </p>
          )}
          <p className="text-xs text-muted-foreground truncate">
            {building.city}, {building.state} {building.zip}
          </p>
        </div>
      </div>

      {/* ── Scrollable body ── */}
      <ScrollArea className="flex-1 sidebar-scroll">
        <div className="px-4 py-3 space-y-4">

          {/* ── Meta badges ── */}
          <div className="flex flex-wrap gap-2 items-center">
            <span
              className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-medium ${typeConf.color}`}
              data-testid="badge-building-type"
            >
              {typeConf.label}
            </span>
            {building.unit_count && (
              <InfoPill icon={Users} text={`${building.unit_count} units`} />
            )}
            {building.year_built && (
              <InfoPill icon={Calendar} text={`Built ${building.year_built}`} />
            )}
            {building.sqft && (
              <InfoPill icon={Ruler} text={`${building.sqft.toLocaleString()} sqft`} />
            )}
          </div>

          {/* ── Owner info ── */}
          {(building.owner_name || building.taxpayer_name) && (
            <div className="space-y-1">
              {building.owner_name && (
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <User className="w-3 h-3 shrink-0" />
                  <span>Owner: <span className="text-foreground">{building.owner_name}</span></span>
                </div>
              )}
              {building.taxpayer_name && building.taxpayer_name !== building.owner_name && (
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <Building2 className="w-3 h-3 shrink-0" />
                  <span>Taxpayer: <span className="text-foreground">{building.taxpayer_name}</span></span>
                </div>
              )}
              {building.data_source && (
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <MapPin className="w-3 h-3 shrink-0" />
                  <span>Source: <span className="text-foreground">{building.data_source.toUpperCase()}</span></span>
                </div>
              )}
            </div>
          )}

          <Separator />

          {/* ── Contacts section ── */}
          <div>
            <div className="flex items-center justify-between gap-2 mb-3">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Contacts
              </h3>
              <Button
                size="sm"
                variant="outline"
                onClick={() => setAddContactOpen(true)}
                data-testid="button-add-contact"
              >
                <Plus className="w-3 h-3 mr-1" />
                Add Contact
              </Button>
            </div>

            {isLoading ? (
              <div className="space-y-2">
                <Skeleton className="h-20 w-full rounded-md" />
                <Skeleton className="h-20 w-full rounded-md" />
              </div>
            ) : data?.contacts.length === 0 ? (
              <p className="text-xs text-muted-foreground py-2">
                No contacts yet. Be the first to add one.
              </p>
            ) : (
              <div className="space-y-2">
                {data?.contacts.map((contact) => (
                  <Card
                    key={contact.id}
                    className="p-3 space-y-2"
                    data-testid={`card-contact-${contact.id}`}
                  >
                    {/* Role + verification status + edit button */}
                    <div className="flex items-center justify-between gap-2">
                      <Badge variant="secondary" className="text-xs shrink-0">
                        {ROLE_LABELS[contact.role] ?? contact.role}
                      </Badge>
                      <div className="flex items-center gap-1 shrink-0">
                        <Button
                          size="icon"
                          variant="ghost"
                          className="h-5 w-5"
                          onClick={() =>
                            setEditingContactId(
                              editingContactId === contact.id ? null : contact.id
                            )
                          }
                          data-testid={`button-edit-contact-${contact.id}`}
                        >
                          <Pencil className="w-3 h-3" />
                        </Button>
                        {contact.confidence === "verified" ? (
                          <CheckCircle2 className="w-3.5 h-3.5 text-[#4ADE80]" />
                        ) : (
                          <Circle className="w-3.5 h-3.5 text-muted-foreground" />
                        )}
                        {contact.source && (
                          <span className="text-[10px] text-muted-foreground">
                            {SOURCE_LABELS[contact.source] ?? contact.source}
                          </span>
                        )}
                      </div>
                    </div>

                    {/* Name */}
                    {contact.name && (
                      <p className="text-sm font-medium leading-none">{contact.name}</p>
                    )}
                    {contact.title && (
                      <p className="text-xs text-muted-foreground">{contact.title}</p>
                    )}

                    {/* Phone / Email */}
                    <div className="flex flex-wrap gap-3">
                      {contact.phone && (
                        <a
                          href={`tel:${contact.phone}`}
                          className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
                          data-testid={`link-contact-phone-${contact.id}`}
                        >
                          <Phone className="w-3 h-3" />
                          {contact.phone}
                        </a>
                      )}
                      {contact.email && (
                        <a
                          href={`mailto:${contact.email}`}
                          className="inline-flex items-center gap-1 text-xs text-primary hover:underline"
                          data-testid={`link-contact-email-${contact.id}`}
                        >
                          <Mail className="w-3 h-3" />
                          {contact.email}
                        </a>
                      )}
                    </div>

                    {/* Notes */}
                    {contact.notes && (
                      <p className="text-xs text-muted-foreground">{contact.notes}</p>
                    )}

                    {/* Inline edit form */}
                    {editingContactId === contact.id && (
                      <EditableContact
                        contact={contact}
                        buildingId={building.id}
                        onDone={() => setEditingContactId(null)}
                      />
                    )}

                    {/* Verify button */}
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-7 text-xs px-2 text-muted-foreground"
                      disabled={verifyContact.isPending}
                      onClick={() =>
                        verifyContact.mutate({ id: contact.id, buildingId: building.id })
                      }
                      data-testid={`button-verify-contact-${contact.id}`}
                    >
                      <CheckCircle2 className="w-3 h-3 mr-1" />
                      Confirm Still Accurate
                      {(contact.verified_count ?? 0) > 0 && (
                        <span className="ml-1 text-[#4ADE80]">
                          ({contact.verified_count})
                        </span>
                      )}
                    </Button>
                  </Card>
                ))}
              </div>
            )}
          </div>

          <Separator />

          {/* ── Field Notes section ── */}
          <div>
            <div className="flex items-center justify-between gap-2 mb-3">
              <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                Field Notes
              </h3>
              <Button
                size="sm"
                variant="outline"
                onClick={() => setAddNoteOpen(true)}
                data-testid="button-add-note"
              >
                <Plus className="w-3 h-3 mr-1" />
                Add Note
              </Button>
            </div>

            {isLoading ? (
              <div className="space-y-2">
                <Skeleton className="h-16 w-full rounded-md" />
              </div>
            ) : data?.notes.length === 0 ? (
              <p className="text-xs text-muted-foreground py-2">
                No field notes yet. Add access intel for your crew.
              </p>
            ) : (
              <div className="space-y-2">
                {data?.notes.map((note) => (
                  <Card
                    key={note.id}
                    className="p-3 space-y-2"
                    data-testid={`card-note-${note.id}`}
                  >
                    <div className="flex items-start justify-between gap-2">
                      <Badge variant="secondary" className="text-xs shrink-0">
                        {NOTE_TYPE_LABELS[note.note_type] ?? note.note_type}
                      </Badge>
                      <div className="flex items-center gap-1 shrink-0">
                        <Button
                          size="icon"
                          variant="ghost"
                          className="h-5 w-5"
                          onClick={() =>
                            setEditingNoteId(
                              editingNoteId === note.id ? null : note.id
                            )
                          }
                          data-testid={`button-edit-note-${note.id}`}
                        >
                          <Pencil className="w-3 h-3" />
                        </Button>
                        <Button
                          size="icon"
                          variant="ghost"
                          className="h-6 w-6"
                          disabled={upvoteNote.isPending}
                          onClick={() =>
                            upvoteNote.mutate({ id: note.id, buildingId: building.id })
                          }
                          data-testid={`button-upvote-note-${note.id}`}
                        >
                          <ThumbsUp className="w-3 h-3" />
                        </Button>
                      </div>
                    </div>
                    <p className="text-xs leading-relaxed" data-testid={`text-note-content-${note.id}`}>
                      {note.content}
                    </p>

                    {/* Inline edit form */}
                    {editingNoteId === note.id && (
                      <EditableNote
                        note={note}
                        buildingId={building.id}
                        onDone={() => setEditingNoteId(null)}
                      />
                    )}

                    <div className="flex items-center justify-between text-[10px] text-muted-foreground">
                      {(note.upvotes ?? 0) > 0 && (
                        <span className="text-[#4ADE80]">{note.upvotes} helpful</span>
                      )}
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </div>

          {/* Bottom padding */}
          <div className="pb-4" />
        </div>
      </ScrollArea>

      {/* ── Dialogs ── */}
      <AddContactForm
        open={addContactOpen}
        onOpenChange={setAddContactOpen}
        buildingId={building.id}
      />
      <AddNoteForm
        open={addNoteOpen}
        onOpenChange={setAddNoteOpen}
        buildingId={building.id}
      />
    </div>
  );
}
